package service

import (
	"context"
	pb "data-server/pb/data"
	"data-server/service/store/index"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupTestServer(t *testing.T) (*grpc.ClientConn, func()) {
	lis := bufconn.Listen(1024 * 1024)

	tmpdir, err := os.MkdirTemp("", "sv-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	idxPath := filepath.Join(tmpdir, "sv.idx")
	logPath := filepath.Join(tmpdir, "sv.log")
	dataPath := filepath.Join(tmpdir, "sv")

	f, err := os.Create(idxPath)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()

	f, err = os.Create(logPath)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()

	s := grpc.NewServer()
	indexStore := index.New(dataPath)
	indexService := New(&indexStore)
	pb.RegisterServiceServer(s, indexService)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Printf("Server exited with error: %v", err)
		}
	}()

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	cleanup := func() {
		conn.Close()
		s.Stop()
		lis.Close()
	}

	return conn, cleanup
}

func parseStream(stream grpc.ServerStreamingClient[pb.Streamed], t *testing.T) {
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Printf("finished parseStream")
			break
		}
		if err != nil {
			t.Errorf("Recv returned error: %v", err)
		}
		switch x := res.Value.(type) {
		case *pb.Streamed_Index:
			log.Printf("Got RecordResult: %T", x)
		case *pb.Streamed_Tag:
			log.Printf("Got Tag: %T", x)
		default:
			log.Printf("Got other result: (%T): %v", x, x)
		}
	}
}

func TestScopeRelay(t *testing.T) {
	conn, cleanup := setupTestServer(t)
	defer cleanup()
	client := pb.NewServiceClient(conn)
	ctx := context.Background()

	req := &pb.WriteScopeRequest{Scope: "test-scope"}
	resp, err := client.WriteScope(ctx, req)
	if err != nil {
		t.Fatalf("%v", err)
	}
	scopeId := resp.GetValue()
	nreq := &pb.WriteNameRequest{
		Names: []*pb.Name{
			&pb.Name{
				Name:    "A",
				ScopeId: scopeId,
				Fields: []*pb.Field{
					&pb.Field{Name: "x", Type: pb.FieldType_INT},
					&pb.Field{Name: "y", Type: pb.FieldType_FLOAT},
				},
			},
		},
	}
	stream, err2 := client.WriteNames(ctx, nreq)
	if err2 != nil {
		t.Fatalf("%v", err)
	}
	parseStream(stream, t)

}


func testWriteData(t *testing.T) {
	conn, cleanup := setupTestServer(t)
	defer cleanup()
	client := pb.NewServiceClient(conn)
	ctx := context.Background()

	req := &pb.WriteScopeRequest{Scope: "test-scope"}
	resp, err := client.WriteScope(ctx, req)
	if err != nil {
		t.Fatalf("%v", err)
	}
	scopeId := resp.GetValue()
    req := &pb.WriteDataRequest
