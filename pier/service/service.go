package service

import (
	"context"

	pb "pier/pb/streamvis/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	store Store
}

func New(st Store) *Service {
	return &Service{
		store: st,
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
	deleted, err := s.store.DeleteScope(ctx, req.GetScopeHandle())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err)
	}
	return &pb.DeleteScopeResponse{Deleted: deleted}, nil
}

func (s *Service) MakeOrGetSeries(
	ctx context.Context,
	req *pb.GetSeriesRequest,
) (*pb.GetSeriesResponse, error) {
	handle, err := s.store.MakeOrGetSeries(
		ctx, req.GetScopeHandle(), req.GetSeriesName(), req.GetSeriesStructure(),
		req.GetDeleteExisting()
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
		ctx, handleUUID, req.GetFieldName(), req.GetFieldVals()
	)
	if err2 != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err)
	}
	return &pb.AppendToSeriesResponse{Success: success}, nil

}
