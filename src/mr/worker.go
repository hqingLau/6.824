package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
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

var workerInfo WorkerInfo
var midFiles []*os.File

func WorkerMap(mapf func(string, string) []KeyValue, c *rpc.Client) {
	// 先尝试一直map，获取文件名，生成中间文件。
	retryTimes := 0
	taskid := 0

	for retryTimes < 3 {

		req := MapRequest{}
		resp := MapResponse{}
		err2 := c.Call("Coordinator.AssignMapTask", &req, &resp)
		if err2 != nil {
			fmt.Printf("err request map work: %v\n", err2)
			time.Sleep(time.Second)
			retryTimes++
			continue
		}
		retryTimes = 0
		if resp.State == "done" {
			fmt.Printf("worker %v map work done.", workerInfo.WorkId)
			return
		}
		if resp.Filename == "" {
			// 为空，但是没done，证明有问题了，等一秒重试
			fmt.Println("map job assign done. but others workers are running")
			time.Sleep(time.Second)
			continue
		}

		// 文件名获取没有问题
		// deal with resp filename
		// write to disk

		req2 := MapTaskState{resp.Filename, workerInfo.WorkId, taskid, "done"}
		resp2 := MapResponse{}

		f, err := os.Open(resp.Filename)
		if err != nil {
			log.Fatal(err)
			req2.State = "nosuchfile"
			c.Call("Coordinator.MapTaskResp", &req2, &resp2)
			continue
		}
		defer f.Close()
		content, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("cannot read %v", resp.Filename)
			req2.State = "filereaderr"
			c.Call("Coordinator.MapTaskResp", &req2, &resp2)
		}

		kvs := mapf(resp.Filename, string(content))

		// enc := json.NewEncoder(out)

		encs := []*json.Encoder{}

		midFiles = []*os.File{}
		// 创建临时文件，确保执行完成了再重命名为mr-mid-{workid}-{taskid}_{nreduceid}
		// 这样就算执行失败了或者重命名过程中执行失败了，存在的中间文件一定是对的
		// reduce时nreduceid号相同的是同一个文件。
		for i := 0; i < workerInfo.NReduce; i++ {
			f, _ := os.CreateTemp("", "di-mp")
			midFiles = append(midFiles, f)
		}

		for i := 0; i < workerInfo.NReduce; i++ {
			encs = append(encs, json.NewEncoder(midFiles[i]))
		}

		for _, kv := range kvs {
			encs[ihash(kv.Key)%workerInfo.NReduce].Encode(kv)
		}

		for i := 0; i < workerInfo.NReduce; i++ {
			name := fmt.Sprintf("%s-%d-%d_%d", "mr-mid", workerInfo.WorkId, taskid, i)
			os.Rename(midFiles[i].Name(), name)
		}

		req2.Filename = resp.Filename
		req2.TaskId = taskid
		req2.State = "done"
		err = c.Call("Coordinator.MapTaskResp", &req2, &resp2)
		if err != nil {
			fmt.Printf("err: %v\n", err)
		}
		taskid++
		// then send to MapTaskResp
		// fmt.Printf("resp: %v\n", resp)

	}
}

func WorkerReduce(reducef func(string, []string) string, client *rpc.Client) {
	// 服务器运行完关闭或者网络有问题，重试
	// 几次后退出

RESTARTREDUCE:

	retryTimes := 0
	for retryTimes < 3 {
		req := ReduceRequest{}
		var resp ReduceResponse
		err2 := client.Call("Coordinator.AssignReduceTask", &req, &resp)

		if err2 != nil {
			fmt.Printf("err request reduce task: %v\n", err2)
			time.Sleep(time.Second)
			retryTimes++
			continue
		}

		retryTimes = 0
		if resp.State == "done" {
			return
		}
		if resp.ReduceId == -1 {
			time.Sleep(time.Second)
			fmt.Println("reduce job assign done. but others workers are running")
			continue
		}

		// 至此，resp.Filename是中间文件的名字了
		// 下一步，读取内容，排序，调用reduce。
		// 类似串行的，只是这里是部分文件，传给
		// coordinate之后再组合吧
		outtmpfile, _ := os.CreateTemp("", "di-out")
		req2 := ReduceTaskState{resp.ReduceId, outtmpfile.Name(), ""}
		resp2 := ReduceResponse{}
		resp2.ReduceId = resp.ReduceId
		fmt.Printf("worker id %v: %v\n", workerInfo.WorkId, resp.Filenames)

		// outFile, _ = os.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0666)

		kva := []KeyValue{}
		jsonParseState := true

		for _, filename := range resp.Filenames {
			f, err := os.Open(filename)
			// fmt.Printf("resp.Filename: %v\n", resp.Filename)
			defer f.Close()
			if err != nil {
				fmt.Println("mid file open wrong:", err)
				req2.State = "nosuchfile"
				client.Call("Coordinator.ReduceStateResp", &req2, &resp2)
				goto RESTARTREDUCE
			}
			d := json.NewDecoder(f)

			for {
				var kv KeyValue
				if err := d.Decode(&kv); err != nil {
					if err == io.EOF {
						break
					}
					fmt.Println("json parse:", err)
					req2.State = "jsonparseerr"
					client.Call("Coordinator.ReduceStateResp", &req2, &resp2)
					jsonParseState = false
					break
				}
				// fmt.Printf("kv: %v\n", kv)
				kva = append(kva, kv)
			}
		}

		if jsonParseState {
			sort.Sort(byKey(kva))

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[i].Key == kva[j].Key {
					j++
				}
				vv := []string{}
				for k := i; k < j; k++ {
					vv = append(vv, kva[k].Value)
				}
				s := reducef(kva[i].Key, vv)
				// 应当都行
				fmt.Fprintf(outtmpfile, "%v %v\n", kva[i].Key, s)
				// fmt.Fprintf(outFiles[ihash(kva[i].Key)%workerInfo.NReduce], "%v %v\n", kva[i].Key, s)
				i = j
			}

		} else {
			goto RESTARTREDUCE
		}

		req2.State = "done"
		req2.TmpOutFilename = outtmpfile.Name()
		req2.ReduceId = resp.ReduceId
		client.Call("Coordinator.ReduceStateResp", &req2, &resp2)
	}
}

type byKey []KeyValue

func (a byKey) Len() int           { return len(a) }
func (a byKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 定义一个uuid
	i := 0
	// c, _ := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, _ := rpc.DialHTTP("unix", sockname)
	defer c.Close()

	c.Call("Coordinator.AssignWorkerId", &i, &workerInfo)

	time.Sleep(time.Second)
	WorkerMap(mapf, c)

	// 至此，mid中间文件全部生成，可向coordinate请求中间文件名
	// 按照mrsequential的做法
	// 接下来就是请求一个中间文件，排序，计数，发送给coordinate
	WorkerReduce(reducef, c)

	ret := false
	for !ret {
		err := c.Call("Coordinator.CoordinatorRPCDone", &i, &ret)
		if err != nil {
			break
		}
	}
	time.Sleep(time.Second * 3)
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

func clearMapMidFiles(midFiles []*os.File) {
	for _, f := range midFiles {
		f.Close()
		os.Remove(f.Name())
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
