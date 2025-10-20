package main

import (
	"flag"
	"fmt"
	"net/http"

	"data-server/pb/streamvis/v1/streamvis_v1connect"
	"data-server/service"
	"data-server/service/store/index"
)

type Server struct{}

func main() {
	port := flag.Int("port", 8001, "Port to listen on")
	dataPath := flag.String("path", "", "/path/to/data holding data.{idx,log}")
	flag.Parse()

	if *dataPath == "" {
		flag.Usage()
		return
	}

	// lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	// if err != nil {
	// log.Fatalf("failed to listen: %v", err)
	// }
	indexStore := index.New(*dataPath)
	indexService := service.New(&indexStore)

	// server := &Server{}
	path, handler := streamvis_v1connect.NewServiceHandler(indexService)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	s := http.Server{
		Addr: fmt.Sprintf("%s:%d", "localhost", *port),
		// Addr:      "localhost:8080",
		Handler:   mux,
		Protocols: p,
	}
	s.ListenAndServe()

	/*
		// grpcServer := grpc.NewServer()
		pb.RegisterServiceServer(grpcServer, indexService)

		reflection.Register(grpcServer)

		log.Printf("gRPC server listening on port %d", *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	*/
}
