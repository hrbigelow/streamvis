package service

import (
	"context"

	pb "pier/pb/streamvis/v1"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	store *Store
}

func NewService(st *Store) *Service {
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
