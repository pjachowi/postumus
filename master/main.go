package main

import (
	"flag"
	"fmt"
	"foobar/postumus/master/impl"
	"foobar/postumus/proto"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var port = flag.Int("port", 50000, "The server port")

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	proto.RegisterMasterServer(s, impl.NewMasterServer(20000))
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}
