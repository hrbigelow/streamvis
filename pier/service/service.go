package service

import (
	"context"
	"regexp"

	pb "pier/pb/streamvis/v1"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	store *Store
}

func NewService(st *Store) *Service {
	return &Service{
		store: st,
	}
}

func streamRecords[M proto.Message, R any](
	ctx context.Context,
	stream connect.ServerStream[R], // Send(*R)
	dataCh <-chan M,
	errCh <-chan error,
	wrapToStream func(msg M) *R,
) error {
	/*
		Consumes the messages in dataCh, wrapping each using wrapToStream, and sending them
		to the stream.  Any error received by errCh or through context cancellation will be
		returned using the appropriate status.Error.
	*/
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

func (s *Service) MakeOrGetScope(
	ctx context.Context,
	req *pb.GetScopeRequest,
) (*pb.GetScopeResponse, error) {
	handle, err := s.store.MakeOrGetScope(ctx, req.GetScopeName(), req.GetDeleteExisting())
	if err != nil {
		return nil, err
	}
	return &pb.GetScopeResponse{ScopeHandle: handle.String()}, nil
}

func (s *Service) DeleteScope(
	ctx context.Context,
	req *pb.DeleteScopeRequest,
) (*pb.DeleteScopeResponse, error) {
	handleUUID, err := uuid.Parse(req.GetScopeHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "SeriesHandle invalid UUID: %v", err)
	}
	deleted, err2 := s.store.DeleteScope(ctx, handleUUID)
	if err2 != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err2)
	}
	return &pb.DeleteScopeResponse{Deleted: deleted}, nil
}

func (s *Service) MakeOrGetSeries(
	ctx context.Context,
	req *pb.GetSeriesRequest,
) (*pb.GetSeriesResponse, error) {
	handleUUID, err := uuid.Parse(req.GetScopeHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ScopeHandle invalid UUID: %v", err)
	}
	handle, err := s.store.MakeOrGetSeries(
		ctx, handleUUID, req.GetSeriesName(), req.GetStructure(),
		req.GetDeleteExisting(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err)
	}
	return &pb.GetSeriesResponse{SeriesHandle: handle.String()}, nil
}

func (s *Service) AppendToSeries(
	ctx context.Context,
	req *pb.AppendToSeriesRequest,
) (*pb.AppendToSeriesResponse, error) {
	handleUUID, err := uuid.Parse(req.GetSeriesHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "SeriesHandle invalid UUID: %v", err)
	}

	success, err2 := s.store.AppendToSeries(
		ctx, handleUUID, req.GetFieldNames(), req.GetFieldVals(),
	)
	if err2 != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err)
	}
	return &pb.AppendToSeriesResponse{Success: success}, nil

}

// TODO: handle NULL regex patterns
func (s *Service) ListScopes(
	ctx context.Context,
	req *pb.ListScopesRequest,
	stream *connect.ServerStream[pb.ListScopesResponse],
) error {
	if _, err := regexp.Compile(req.ScopeRegex); err != nil {
		return status.Errorf(codes.InvalidArgument, "ScopeRegex invalid: %v", err)
	}
	if _, err := regexp.Compile(req.SeriesRegex); err != nil {
		return status.Errorf(codes.InvalidArgument, "SeriesRegex invalid: %v", err)
	}
	dataCh, errCh := s.store.ListScopes(ctx, req.ScopeRegex, req.SeriesRegex, req.WithStats)
	wrapData := func(msg *pb.Scope) *pb.ListScopesResponse {
		return &pb.ListScopesResponse{
			Scope: msg,
		}
	}
	return streamRecords[*pb.Scope, pb.ListScopesResponse](ctx, *stream, dataCh, errCh, wrapData)
}
