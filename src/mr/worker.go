package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 定义一个uuid
	uuid, _ := uuid.NewUUID()

	// 先尝试一直map，获取文件名，生成中间文件。
	retryTimes := 0
	for retryTimes < 10 {
		c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
		if err != nil {
			log.Fatal("rpc err: ", err)
			time.Sleep(time.Second)
			retryTimes++
			continue
		}
		retryTimes = 0
		req := MapRequest{uuid.String()}
		resp := MapResponse{}
		err2 := c.Call("Coordinator.GetInputFile", &req, &resp)
		if err2 != nil {
			fmt.Printf("err2: %v\n", err2)
		}

		if resp.State == "done" {
			break
		}
		if resp.Filename == "" {
			// 为空，但是没done，证明有问题了，等一秒重试
			time.Sleep(time.Second)
			continue
		}

		// 文件名获取没有问题
		// deal with resp filename
		// write to disk

		req2 := MapTaskState{resp.Filename, "done"}
		resp2 := MapResponse{}

		f, err := os.Open(resp.Filename)
		if err != nil {
			log.Fatal(err)
			req2.State = "nosuchfile"
			c.Call("Coordinator.MapInputFileResp", &req2, &resp2)
			return
		}
		defer f.Close()
		content, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", resp.Filename)
			req2.State = "filereaderr"
			c.Call("Coordinator.MapInputFileResp", &req2, &resp2)
		}

		kvs := mapf(resp.Filename, string(content))
		fmt.Printf("uuid: %v\n", uuid)
		outFilename := fmt.Sprintf("%s%d", "mr-out-", ihash(uuid.String())%10)
		fmt.Printf("outFilename: %v\n", outFilename)
		out, _ := os.Create(outFilename)
		defer out.Close()
		enc := json.NewEncoder(out)
		for _, kv := range kvs {
			enc.Encode(kv)
		}

		req2.Filename = outFilename + "+" + resp.Filename
		req2.State = "done"
		err = c.Call("Coordinator.MapInputFileResp", &req2, &resp2)
		if err != nil {
			fmt.Printf("err: %v\n", err)
		}
		// then send to MapInputFileResp
		fmt.Printf("resp: %v\n", resp)

	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
