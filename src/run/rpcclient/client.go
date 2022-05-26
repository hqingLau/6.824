package main

import (
	"fmt"
	"log"
	"net/rpc"
)

// 参数定义都需要的
type RpcRequest struct {
	A, B int
}

type RpcResponse struct {
	Quo, Rem int
}

func main() {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing: ", err)
	}
	// 同步调用，等待请求结束
	req := &RpcRequest{9, 4}
	var reply int
	c.Call("SomeType.Multiply", req, &reply)
	fmt.Printf("reply: %v\n", reply)

	// 异步调用，返回一个channel
	response := new(RpcResponse)
	c2 := c.Go("SomeType.Divide", req, &response, nil)
	fmt.Println("do something else")
	<-c2.Done
	fmt.Printf("response: %v\n", response)

	// output
	// reply: 36
	// do something else
	// response: &{2 1}
}
