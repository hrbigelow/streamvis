package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	pb "data-server/pb/data"
	"data-server/service"
	"data-server/service/store/index"

	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 8001, "Port to listen on")
	dataPath := flag.String("path", "", "/path/to/data holding data.{idx,log}")
	flag.Parse()

	if *dataPath == "" {
		flag.Usage()
		return
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	indexStore := index.New(*dataPath)
	indexService := service.New(&indexStore)
	pb.RegisterServiceServer(grpcServer, indexService)

	log.Printf("gRPC server listening on port %d", *port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
