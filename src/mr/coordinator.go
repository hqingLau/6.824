package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

var void interface{}

type Coordinator struct {
	// filename list
	files []string

	// 通过rpc发送了，但是还没有收到完成信号的
	mapSend    map[string]interface{}
	reduceSend map[string]interface{}
	nReduce    int

	// 保护files
	mtx sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// map调用，返回第一个文件名
func (c *Coordinator) GetInputFile(req *MapRequest, resp *MapResponse) error {
	c.mtx.Lock()
	if len(c.files) == 0 {
		resp.Filename = ""
		if len(c.mapSend) == 0 {
			resp.State = "done"
		}
	} else {
		resp.Filename = c.files[0]
		c.files = c.files[1:]
		c.mapSend[resp.Filename] = void

	}
	c.mtx.Unlock()
	fmt.Printf("c.files: %v\n", c.files)
	return nil
}

// map处理完毕了调用，防止某一个处理过程中崩溃,如果彻底崩溃了，设置10s超时，再把这个
// 元素加回去
func (c *Coordinator) MapInputFileResp(state *MapTaskState, resp *MapResponse) error {
	c.mtx.Lock()
	if state.state == "done" {
		// ok了，删除这个元素
		delete(c.mapSend, state.filename)
	} else {
		// 处理没成功，重新处理
		c.files = append(c.files, state.filename)
		// 不删除，这样状态一直存在，知道再次被分配了，然后delete
		// delete(c.mapSend, state.filename)
	}
	c.mtx.Unlock()
	fmt.Printf("c.files: %v\n", c.files)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.files = files
	c.mtx = sync.Mutex{}
	c.mapSend = make(map[string]interface{})
	c.reduceSend = make(map[string]interface{})

	c.server()
	fmt.Printf("c: %v\n", c)
	return &c
}
