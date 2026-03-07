package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"pier/pb/streamvis/v1/streamvis_v1connect"
	"pier/service"

	"connectrpc.com/grpcreflect"
)

func main() {
	dbUri := os.Getenv("STREAMVIS_DB_URI")
	if dbUri == "" {
		fmt.Fprintf(os.Stderr, "Please define STREAMVIS_DB_URI\n")
		os.Exit(1)
	}
	port := flag.Int("port", 8001, "Port to listen on")
	doTraceDb := flag.Bool("debug", false, "enable debugging")

	flag.Parse()

	store, err := service.NewStore(context.Background(), dbUri, *doTraceDb)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create Store: %v\n", err)
		os.Exit(1)
	}
	dbService := service.NewService(store)

	mux := http.NewServeMux()
	path, serviceHandler := streamvis_v1connect.NewServiceHandler(dbService)
	nakedPath := strings.ReplaceAll(path, "/", "")
	fmt.Printf("URI: localhost:%d, Service: %s\n", *port, nakedPath)

	mux.Handle(path, serviceHandler)

	reflector := grpcreflect.NewStaticReflector(nakedPath)

	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	s := http.Server{
		Addr:      fmt.Sprintf("%s:%d", "", *port),
		Handler:   mux,
		Protocols: p,
	}
	if err := s.ListenAndServe(); err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			log.Println("Server closed gracefully")
		} else {
			log.Fatalf("Server error: %v", err)
		}
	}

	/*
		var greeting string
		err = dbpool.QueryRow(context.Background(), "SELECT 'Hello, World!'").Scan(&greeting)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println(greeting)
	*/

}
