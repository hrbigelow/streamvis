package main

import (
	"flag"
	"fmt"
	"net/http"

	"connectrpc.com/grpcreflect"

	"data-server/pb/streamvis/v1/streamvis_v1connect"
	"data-server/service"
	"data-server/service/store/index"
)

func main() {
	port := flag.Int("port", 8001, "Port to listen on")
	dataPath := flag.String("path", "", "/path/to/data holding data.{idx,log}")
	flag.Parse()

	if *dataPath == "" {
		flag.Usage()
		return
	}

	// provide a single global in-memory index supporting all queries
	indexStore := index.New(*dataPath)
	indexService := service.New(&indexStore)

	mux := http.NewServeMux()

	path, serviceHandler := streamvis_v1connect.NewServiceHandler(indexService)
	fmt.Printf("Handler path: %s\n", path)

	mux.Handle(path, serviceHandler)

	reflector := grpcreflect.NewStaticReflector(
		"streamvis.v1.Service",
	)

	/*
		debugHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Printf("Incoming request: %s %s\n", r.Method, r.URL.Path)
			fmt.Printf("Content-Type: %s\n", r.Header.Get("Content-Type"))
			fmt.Printf("Proto: %s\n", r.Proto)
			serviceHandler.ServeHTTP(w, r)
		})
		mux.Handle(path, debugHandler)
	*/

	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

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
}
