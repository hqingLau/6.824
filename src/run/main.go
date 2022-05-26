package main

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type RpcRequest struct {
	A, B int
}

type RpcResponse struct {
	Quo, Rem int
}

type SomeType int

func (t *SomeType) Multiply(req *RpcRequest, response *int) error {
	*response = req.A * req.B
	return nil
}

func (t *SomeType) Divide(req *RpcRequest, response *RpcResponse) error {
	if req.B == 0 {
		return errors.New("divided by 0")
	}
	response.Quo = req.A / req.B
	response.Rem = req.A % req.B
	return nil
}

func main() {
	st := new(SomeType)
	// 注册rpc服务
	rpc.Register(st)
	// rpc服务挂载到http服务上
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error: ", err)
	}
	// http服务打开后就可通过rpc客户端调用方法
	http.Serve(l, nil)
}
