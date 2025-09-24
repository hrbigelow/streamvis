package service

import (
	"regexp"

	pb "data-server/pb/data"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Service struct {
	pb           pb.UnimplementedServiceServer
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

func (s *Service) Scopes(req emptypb.Empty, stream pb.Service_ScopesServer) error {
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
