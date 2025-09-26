package service

import (
	"context"
	"regexp"

	pb "data-server/pb/data"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
	pb.UnimplementedServiceServer
	store        Store
	lastIssuedId uint32
}

func New(st Store) *Service {
	return &Service{
		store:        st,
		lastIssuedId: st.GetMaxId(),
	}
}

func (s *Service) IssueId() uint32 {
	s.lastIssuedId += 1
	return s.lastIssuedId
}

func streamRecords[M proto.Message, R any](
	stream grpc.ServerStreamingServer[R], // Send(*R)
	dataCh <-chan M,
	errCh <-chan error,
	wrapToStream func(msg M) *R,
) error {
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return status.Convert(ctx.Err()).Err()

		case err, ok := <-errCh:
			if !ok {
				// error channel closed without error.
				return nil
			}

			st := status.Convert(err)
			if st.Code() == codes.OK {
				st = status.New(codes.Internal, err.Error())
			}
			stream.SetTrailer(metadata.Pairs("x-partial", "true"))
			return st.Err()

		case d, ok := <-dataCh:
			if !ok {
				// data channel closed cleanly
				return nil
			}

			if err := stream.Send(wrapToStream(d)); err != nil {
				return status.Errorf(codes.Unavailable, "send failed: %v", err)
			}
		}
	}
}

func (s *Service) QueryRecords(
	req *pb.RecordRequest,
	stream pb.Service_QueryRecordsServer,
) error {
	scopePat, err := regexp.Compile(req.GetScopePattern())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad scope_regex: %v", err)
	}
	namePat, err := regexp.Compile(req.GetNamePattern())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad name_regex: %v", err)
	}
	// TODO: send some RecordResult as well, useful for reconstruction

	ctx := stream.Context()
	dataCh, errCh := s.store.GetData(scopePat, namePat, req.GetFileOffset(), ctx)
	wrapData := func(msg *pb.Data) *pb.StreamedRecord {
		return &pb.StreamedRecord{
			Record: &pb.StreamedRecord_Data{Data: msg},
		}
	}
	return streamRecords[*pb.Data, pb.StreamedRecord](stream, dataCh, errCh, wrapData)
}

func (s *Service) Configs(
	req *pb.ScopeRequest,
	stream pb.Service_ConfigsServer,
) error {
	scopePat, err := regexp.Compile("^" + req.GetScope() + "$")
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad scope name: %v", err)
	}
	// TODO: send some RecordResult as well, useful for reconstruction

	ctx := stream.Context()
	dataCh, errCh := s.store.GetConfigs(scopePat, 0, ctx)
	wrapConfig := func(msg *pb.Config) *pb.StreamedRecord {
		return &pb.StreamedRecord{
			Record: &pb.StreamedRecord_Config{Config: msg},
		}
	}
	return streamRecords[*pb.Config, pb.StreamedRecord](stream, dataCh, errCh, wrapConfig)
}

func (s *Service) Scopes(req *emptypb.Empty, stream pb.Service_ScopesServer) error {
	ctx := stream.Context()
	scopePat, _ := regexp.Compile(".*")
	scopes := s.store.GetScopes(scopePat)
	for _, scope := range scopes {
		select {
		case <-ctx.Done():
			return status.Convert(ctx.Err()).Err()
		default:
			// continue
		}
		msg := &pb.StreamedRecord{
			Record: &pb.StreamedRecord_Value{Value: scope},
		}
		if err := stream.Send(msg); err != nil {
			return status.Errorf(codes.Unavailable, "send failed: %v", err)
		}
	}
	return nil
}

func (s *Service) Names(req *pb.ScopeRequest, stream pb.Service_NamesServer) error {
	ctx := stream.Context()
	scopePat, err := regexp.Compile("^" + req.GetScope() + "$")
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad scope name: %v", err)
	}
	namePat, _ := regexp.Compile(".*")
	tags := s.store.GetNames(scopePat, namePat)
	for _, tag := range tags {
		select {
		case <-ctx.Done():
			return status.Convert(ctx.Err()).Err()
		default:
			// continue
		}
		msg := &pb.StreamedRecord{
			Record: &pb.StreamedRecord_Tag{
				Tag: &pb.Tag{Scope: tag[0], Name: tag[1]},
			},
		}
		if err := stream.Send(msg); err != nil {
			return status.Errorf(codes.Unavailable, "send failed: %v", err)
		}
	}
	return nil
}

func (s *Service) WriteScope(
	ctx context.Context,
	req *pb.WriteScopeRequest,
) (*pb.IntegerResponse, error) {
	msg := &pb.Scope{
		ScopeId: s.IssueId(),
		Scope:   req.GetScope(),
		Time:    timestamppb.Now(),
	}
	s.store.Add(msg)
	return &pb.IntegerResponse{Value: msg.GetScopeId()}, nil
}

func (s *Service) WriteConfig(
	ctx context.Context,
	req *pb.WriteConfigRequest,
) (*emptypb.Empty, error) {
	msg := &pb.Config{
		EntryId:    s.IssueId(),
		Attributes: req.Attributes,
	}
	s.store.Add(msg)
	return &emptypb.Empty{}, nil
}

func (s *Service) WriteNames(req *pb.WriteNameRequest, stream pb.Service_WriteNamesServer) error {
	// assigns new NameId to each Name message, stores and returns them
	ptrs := make([]*pb.Name, len(req.Names))
	for i := range req.Names {
		req.Names[i].NameId = s.IssueId()
		ptrs[i] = req.Names[i]
	}
	s.store.AddNames(req.Names)
	for i := range req.Names {
		msg := &pb.StreamedRecord{
			Record: &pb.StreamedRecord_Name{
				Name: ptrs[i],
			},
		}
		if err := stream.Send(msg); err != nil {
			return status.Errorf(codes.Unavailable, "send failed: %v", err)
		}
	}
	return nil
}

func (s *Service) DeleteScopeNames(
	ctx context.Context,
	req *pb.ScopeNameRequest,
) (*emptypb.Empty, error) {
	msgs := make([]*pb.Control, len(req.Names))
	for i, name := range req.Names {
		msg := &pb.Control{
			Scope:  req.Scope,
			Name:   name,
			Action: pb.Action_DELETE_NAME,
		}
		msgs[i] = msg
	}
	// TODO: need to actually update the in-memory index here
	//s.store.Add(msg)
	return &emptypb.Empty{}, nil
}

func (s *Service) WriteData(
	ctx context.Context,
	req *pb.WriteDataRequest,
) (*emptypb.Empty, error) {
	for i := range req.Datas {
		req.Datas[i].EntryId = s.IssueId()
	}
	s.store.AddDatas(req.Datas)
	return &emptypb.Empty{}, nil
}
