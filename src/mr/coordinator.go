package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var void interface{}

type stringArray []string

type Coordinator struct {
	// filename list
	files        []string
	reduceId     int                 //分配给worker的id号
	midFilesMap  map[int]stringArray //reduceId:stringArray
	midFilesList []int
	// 通过rpc发送了，但是还没有收到完成信号的
	mapSend    map[string]*time.Timer
	reduceSend map[int]*time.Timer //key: reduceId
	nReduce    int
	ok         bool // 任务是否完毕
	// 保护files
	mtx sync.Mutex
}

func (c *Coordinator) AssignWorkerId(i *int, wi *WorkerInfo) error {
	c.mtx.Lock()
	wi.WorkId = c.reduceId
	wi.NReduce = c.nReduce
	c.reduceId++
	c.mtx.Unlock()
	return nil
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

func mapAfterFuncWrapper(c *Coordinator, filename string) func() {
	return func() {
		c.mtx.Lock()
		fmt.Printf("map task %v 超时重试\n", filename)
		c.files = append(c.files, filename)
		c.mtx.Unlock()
	}
}

func reduceAfterFuncWrapper(c *Coordinator, reduceId int) func() {
	return func() {
		c.mtx.Lock()
		fmt.Printf("reduce task %v 超时重试\n", reduceId)
		c.midFilesList = append(c.midFilesList, reduceId)
		c.mtx.Unlock()
	}
}

// map调用，返回第一个文件名
func (c *Coordinator) AssignMapTask(req *MapRequest, resp *MapResponse) error {
	c.mtx.Lock()
	if len(c.files) == 0 {
		resp.Filename = ""
		if len(c.mapSend) == 0 {
			resp.State = "done"
			fmt.Println("map task done.")
		}
	} else {
		resp.Filename = c.files[0]
		c.files = c.files[1:]
		f := mapAfterFuncWrapper(c, resp.Filename)
		c.mapSend[resp.Filename] = time.AfterFunc(time.Second*10, f)
	}
	c.mtx.Unlock()
	// fmt.Printf("c.files: %v\n", c.files)
	fmt.Printf("c.files: %v\n", c.files)
	fmt.Printf("c.mapSend: %v\n", c.mapSend)
	return nil
}

// reduce之前，返回中间文件的文件名d
func (c *Coordinator) AssignReduceTask(req *ReduceRequest, resp *ReduceResponse) error {
	c.mtx.Lock()
	if len(c.midFilesList) == 0 {
		resp.Filenames = nil
		resp.ReduceId = -1
		if len(c.reduceSend) == 0 {
			resp.State = "done"
			fmt.Println("reduce task done")
		}
	} else {
		resp.ReduceId = c.midFilesList[0]
		c.midFilesList = c.midFilesList[1:]
		resp.Filenames = c.midFilesMap[resp.ReduceId]
		f := reduceAfterFuncWrapper(c, resp.ReduceId)
		c.reduceSend[resp.ReduceId] = time.AfterFunc(time.Second*10, f)
	}
	fmt.Printf("c.midFilesList: %v\n", c.midFilesList)
	fmt.Printf("c.reduceSend: %v\n", c.reduceSend)
	c.mtx.Unlock()
	return nil
}

// map处理完毕了调用，防止某一个处理过程中崩溃,如果彻底崩溃了，设置10s超时，再把这个
// 元素加回去, 全部文件结束了之后，得到全部的中间文件名list。
func (c *Coordinator) MapTaskResp(state *MapTaskState, resp *MapResponse) error {
	c.mtx.Lock()
	// fmt.Printf("state: %v\n", state)
	// 如果这个文件已经处理完了，直接跳过，不关心这次处理的结果
	_, ok := c.mapSend[state.Filename]
	if !ok {
		// 已经有人删除了这个元素，也就是工作完成了
		c.mtx.Unlock()
		return nil
	}
	if state.State == "done" {
		// ok了，删除这个元素,对应的定时器也没用了，关掉
		c.mapSend[state.Filename].Stop()
		delete(c.mapSend, state.Filename)
		fmt.Printf("timer delete +++++++++++++++++: %v\n", state.Filename)

		for i := 0; i < c.nReduce; i++ {
			name := fmt.Sprintf("%s-%d-%d_%d", "mr-mid", state.WorkerId, state.TaskId, i)
			_, ok := c.midFilesMap[i]
			if !ok {
				c.midFilesMap[i] = stringArray{}
			}
			c.midFilesMap[i] = append(c.midFilesMap[i], name)
		}

		// fmt.Printf("names[0]: %v\n", names[0])
	} else {
		// 处理没成功，重新处理
		c.files = append(c.files, state.Filename)
		// 从超时列表里去掉

		// 不删除，这样状态一直存在，知道再次被分配了，然后delete
		// delete(c.mapSend, state.filename)
	}
	c.mtx.Unlock()
	return nil
}

func (c *Coordinator) ReduceStateResp(state *ReduceTaskState, resp *ReduceResponse) error {
	c.mtx.Lock()
	// fmt.Printf("state: %v\n", state)
	_, ok := c.reduceSend[state.ReduceId]
	if !ok {
		c.mtx.Unlock()
		return nil
	}
	if state.State == "done" {
		// ok了，删除这个元素
		c.reduceSend[state.ReduceId].Stop()
		delete(c.reduceSend, state.ReduceId)
		reduceId := state.ReduceId
		name := fmt.Sprintf("%s-%d", "mr-out", reduceId)
		os.Rename(state.TmpOutFilename, name)

		// fmt.Printf("c.reduceSend: %v\n", c.reduceSend)
		if len(c.reduceSend) == 0 && len(c.midFilesList) == 0 && !c.ok {
			c.ok = true
		}
	} else {
		// 处理没成功，重新处理
		c.midFilesList = append(c.midFilesList, state.ReduceId)
		// 不删除，这样状态一直存在，知道再次被分配了，然后delete
		// delete(c.mapSend, state.filename)
	}
	c.mtx.Unlock()

	// if len(c.reduceSend) == 0 && len(c.midFiles) == 0 && !c.ok {
	// 	// 组合
	// 	go func() {
	// 		c.mtx.Lock()
	// 		if c.ok {
	// 			c.mtx.Unlock()
	// 			return
	// 		}
	// 		combineReduceFiles()
	// 		c.ok = true
	// 		c.mtx.Unlock()
	// 	}()
	// }

	// fmt.Printf("c.midfiles: %v\n", c.midFiles)
	return nil
}

// func combineReduceFiles() {
// 	matches, _ := filepath.Glob("mr-reduceout*")
// 	fmt.Println(matches)
// 	allWords := make(map[string]int)
// 	for _, midfile := range matches {
// 		fmt.Println("dealing ", midfile, "....")
// 		f, _ := os.Open(midfile)
// 		fileScanner := bufio.NewScanner(f)
// 		for fileScanner.Scan() {
// 			s := strings.Split(fileScanner.Text(), " ")
// 			_, ok := allWords[s[0]]
// 			if !ok {
// 				allWords[s[0]] = 0
// 			}
// 			wc, _ := strconv.Atoi(s[1])
// 			allWords[s[0]] += wc
// 		}
// 	}

// 	f, _ := os.Create("mr-out-X")
// 	ks := make([]string, len(allWords))
// 	i := 0
// 	for k, _ := range allWords {
// 		ks[i] = k
// 		i++
// 	}
// 	sort.Strings(ks)
// 	for _, k := range ks {
// 		fmt.Fprintf(f, "%v %v\n", k, allWords[k])
// 		// fmt.Printf("%v %v\n", k, allWords[k])
// 	}
// }

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	c.mtx.Lock()
	ret := c.ok
	c.mtx.Unlock()

	// Your code here.

	return ret
}

func (c *Coordinator) CoordinatorRPCDone(i *int, ret *bool) error {
	c.mtx.Lock()
	t := c.ok
	c.mtx.Unlock()

	*ret = t
	return nil
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
	c.midFilesMap = map[int]stringArray{}
	c.midFilesList = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	c.mtx = sync.Mutex{}
	c.mapSend = make(map[string]*time.Timer)
	c.reduceSend = make(map[int]*time.Timer)
	c.server()
	fmt.Printf("c: %v\n", c)
	return &c
}
