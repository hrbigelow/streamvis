package service

import (
	"context"

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

func (s *Service) MakeOrGetSeries(
	ctx context.Context,
	req *pb.GetSeriesRequest,
) (*pb.GetSeriesResponse, error) {
	handle, err := s.store.MakeOrGetSeries(
		ctx, req.GetSeriesName(), req.GetStructure(),
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
	seriesHandleUUID, err := uuid.Parse(req.GetSeriesHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "SeriesHandle invalid UUID: %v", err)
	}
	runHandleUUID, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RunHandle invalid UUID: %v", err)
	}

	success, err := s.store.AppendToSeries(
		ctx, seriesHandleUUID, runHandleUUID, req.GetFieldNames(), req.GetFieldVals(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err)
	}
	return &pb.AppendToSeriesResponse{Success: success}, nil

}

func (s *Service) CreateRun(
	ctx context.Context,
	req *pb.CreateRunRequest,
) (*pb.CreateRunResponse, error) {
	handle, err := s.store.CreateRun(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}
	return &pb.CreateRunResponse{RunHandle: handle.String()}, nil
}

func (s *Service) DeleteRun(
	ctx context.Context,
	req *pb.DeleteRunRequest,
) (*pb.DeleteRunResponse, error) {
	handle, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RunHandle invalid UUID: %v", err)
	}
	success, err := s.store.DeleteRun(ctx, handle)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}
	return &pb.DeleteRunResponse{Success: success}, nil
}
