package service

import (
	"context"
	"fmt"
	"regexp"

	pb "data-server/pb/streamvis/v1"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Service struct {
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
	ctx context.Context,
	stream connect.ServerStream[R], // Send(*R)
	dataCh <-chan M,
	errCh <-chan error,
	wrapToStream func(msg M) *R,
) error {
	for {
		select {
		case <-ctx.Done():
			return status.Convert(ctx.Err()).Err()

		case err, ok := <-errCh:
			if err != nil && ok {
				st := status.Convert(err)
				if st.Code() == codes.OK {
					st = status.New(codes.Internal, err.Error())
				}
				stream.ResponseTrailer().Set("x-partial", "true")
				return st.Err()
			}

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

/*
QueryData finds and returns all Data items in the database whose scope and name
matches req.scope_pattern and req.name_pattern, and which occur at or after
req.file_offset in the backing data file.  It returns a pb.RecordResult.  The
result file_offset can be then used for the next request to retrieve records
incrementally.  The pb.RecordResult scopes and names maps represent the current
state of the index up until the file_offset, and consistent with the scope_pattern
and name_pattern filters
*/
func (s *Service) QueryData(
	ctx context.Context,
	req *pb.DataRequest,
	stream *connect.ServerStream[pb.DataResult],
) error {
	scopePat, err := regexp.Compile(req.GetScopePattern())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad scope_regex: %v", err)
	}
	namePat, err := regexp.Compile(req.GetNamePattern())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad name_regex: %v", err)
	}

	res, dataCh, errCh := s.store.GetData(scopePat, namePat, req.FileOffset, ctx)
	dres := &pb.DataResult{Value: &pb.DataResult_Record{Record: &res}}
	stream.Send(dres)

	wrapData := func(msg *pb.Data) *pb.DataResult {
		return &pb.DataResult{Value: &pb.DataResult_Data{Data: msg}}
	}
	return streamRecords[*pb.Data, pb.DataResult](ctx, *stream, dataCh, errCh, wrapData)
}

// Configs streams all Config objects matching req.scope, as well as a RecordResult
// of the Scope objects owning the Config objects
func (s *Service) Configs(
	ctx context.Context,
	req *pb.ConfigRequest,
	stream *connect.ServerStream[pb.ConfigResult],
	// stream pb.Service_ConfigsServer,
) error {
	scopePat, err := regexp.Compile("^" + req.GetScope() + "$")
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "bad scope name: %v", err)
	}
	res, dataCh, errCh := s.store.GetConfigs(scopePat, ctx)
	cres := &pb.ConfigResult{Value: &pb.ConfigResult_Index{Index: &res}}
	// streamed := util.WrapStreamed(&res)
	stream.Send(cres)
	// stream.Send(streamed)
	wrapConfig := func(msg *pb.Config) *pb.ConfigResult {
		return &pb.ConfigResult{Value: &pb.ConfigResult_Config{Config: msg}}
	}

	return streamRecords[*pb.Config, pb.ConfigResult](ctx, *stream, dataCh, errCh, wrapConfig)
}

func (s *Service) Scopes(
	ctx context.Context,
	req *pb.ScopeRequest,
	stream *connect.ServerStream[pb.ScopeResult],
) error {
	scopePat, _ := regexp.Compile(".*")
	scopes := s.store.GetScopes(scopePat)
	for _, scope := range scopes {
		select {
		case <-ctx.Done():
			return status.Convert(ctx.Err()).Err()
		default:
			// continue
		}
		msg := &pb.ScopeResult{Scope: scope}
		if err := stream.Send(msg); err != nil {
			return status.Errorf(codes.Unavailable, "send failed: %v", err)
		}
	}
	return nil
}

func (s *Service) Names(
	ctx context.Context,
	req *pb.NamesRequest,
	stream *connect.ServerStream[pb.Tag],
) error {
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
		msg := &pb.Tag{Scope: tag[0], Name: tag[1]}
		if err := stream.Send(msg); err != nil {
			return status.Errorf(codes.Unavailable, "send failed: %v", err)
		}
	}
	return nil
}

func (s *Service) WriteScope(
	ctx context.Context,
	req *pb.WriteScopeRequest,
) (*pb.WriteScopeResponse, error) {
	msg := &pb.Scope{
		ScopeId: s.IssueId(),
		Scope:   req.GetScope(),
		Time:    timestamppb.Now(),
	}
	if err := s.store.AddScope(msg); err != nil {
		return nil, status.Errorf(codes.Unavailable, "WriteScope failed: %v", err)
	}
	return &pb.WriteScopeResponse{ScopeId: msg.GetScopeId()}, nil
}

func (s *Service) WriteConfig(
	ctx context.Context,
	req *pb.WriteConfigRequest,
) (*pb.WriteConfigResponse, error) {
	msg := &pb.Config{
		EntryId:    s.IssueId(),
		Attributes: req.GetAttributes(),
		ScopeId:    req.GetScopeId(),
	}
	if err := s.store.AddConfig(msg); err != nil {
		return nil, status.Errorf(codes.Unavailable, "WriteConfig failed: %v", err)
	}
	return &pb.WriteConfigResponse{}, nil
}

// WriteNames persists req.Names to the store.  Each Name object must
// be fully populated except for NameId, which is issued automatically by this
// request.  The Name objects are streamed back to the client with their NameId
// populated.
func (s *Service) WriteNames(
	_ context.Context,
	req *pb.WriteNameRequest,
) (*pb.WriteNameResponse, error) {
	// assigns new NameId to each Name message, stores and returns them
	ptrs := make([]*pb.Name, len(req.Names))
	for i := range req.Names {
		req.Names[i].NameId = s.IssueId()
		ptrs[i] = req.Names[i]
	}
	s.store.AddNames(req.Names)

	res := &pb.WriteNameResponse{}

	for _, name := range req.Names {
		res.Names = append(res.Names, name)
	}
	return res, nil
}

func (s *Service) DeleteScopeNames(
	_ context.Context,
	req *pb.DeleteTagRequest,
) (*pb.DeleteTagResponse, error) {
	s.store.DeleteScopeNames(req.Scope, req.Names)
	return &pb.DeleteTagResponse{}, nil
}

func (s *Service) WriteData(
	ctx context.Context,
	req *pb.WriteDataRequest,
) (*pb.WriteDataResponse, error) {
	for i := range req.Datas {
		req.Datas[i].EntryId = s.IssueId()
	}
	if err := s.store.AddDatas(req.Datas); err != nil {
		return &pb.WriteDataResponse{}, fmt.Errorf("WriteData: couldn't AddDatas: %v", err)
	}

	return &pb.WriteDataResponse{}, nil
}
