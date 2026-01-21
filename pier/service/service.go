package service

import (
	"context"
	"time"

	pb "pier/pb/streamvis/v1"

	"connectrpc.com/connect"
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

func streamRecords[R any](
	ctx context.Context,
	stream connect.ServerStream[R], // Send(*R)
	dataCh <-chan *R,
	errCh <-chan error,
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

			if err := stream.Send(d); err != nil {
				return status.Errorf(codes.Unavailable, "send failed: %v", err)
			}
		}
	}
}

func parseUUIDs(uuidStr []string, tag string) ([]uuid.UUID, error) {
	objs := make([]uuid.UUID, len(uuidStr))
	for i, str := range uuidStr {
		obj, err := uuid.Parse(str)
		if err != nil {
			return objs, status.Errorf(codes.InvalidArgument, "%s is invalid UUID: %v", tag, err)
		}
		objs[i] = obj
	}
	return objs, nil
}

func (s *Service) CreateField(
	ctx context.Context,
	req *pb.CreateFieldRequest,
) (*pb.CreateFieldResponse, error) {
	err := s.store.CreateField(
		ctx, req.GetName(), req.GetDataType(), req.GetDescription(),
	)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &pb.CreateFieldResponse{}, nil
}

func (s *Service) CreateSeries(
	ctx context.Context,
	req *pb.CreateSeriesRequest,
) (*pb.CreateSeriesResponse, error) {
	err := s.store.CreateSeries(
		ctx, req.GetSeriesName(), req.GetFieldNames(),
	)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &pb.CreateSeriesResponse{}, nil
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

	err = s.store.AppendToSeries(
		ctx, seriesHandleUUID, runHandleUUID, req.GetFieldVals(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database store error: %v", err)
	}
	return &pb.AppendToSeriesResponse{}, nil

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

func (s *Service) ReplaceRun(
	ctx context.Context,
	req *pb.ReplaceRunRequest,
) (*pb.ReplaceRunResponse, error) {
	handle, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RunHandle invalid UUID: %v", err)
	}
	err = s.store.ReplaceRun(ctx, handle)
	return &pb.ReplaceRunResponse{}, err
}

func (s *Service) DeleteRun(
	ctx context.Context,
	req *pb.DeleteRunRequest,
) (*pb.DeleteRunResponse, error) {
	handle, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RunHandle invalid UUID: %v", err)
	}
	err = s.store.DeleteRun(ctx, handle)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}
	return &pb.DeleteRunResponse{}, nil
}

func (s *Service) SetRunAttributes(
	ctx context.Context,
	req *pb.SetRunAttributesRequest,
) (*pb.SetRunAttributesResponse, error) {
	handle, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RunHandle invalid UUID: %v", err)
	}
	err = s.store.SetRunAttributes(ctx, handle, req.GetAttrs())
	if err != nil {
		return nil, err
	}
	return &pb.SetRunAttributesResponse{}, nil
}

func (s *Service) ListSeries(
	ctx context.Context,
	req *pb.ListSeriesRequest,
	stream *connect.ServerStream[pb.Series],
) error {
	dataCh, errCh := s.store.ListSeries(ctx)
	return streamRecords[pb.Series](ctx, *stream, dataCh, errCh)
}

func (s *Service) DeleteEmptySeries(
	ctx context.Context,
	req *pb.DeleteEmptySeriesRequest,
) (*pb.DeleteEmptySeriesResponse, error) {
	err := s.store.DeleteEmptySeries(ctx, req.GetSeriesName())
	return &pb.DeleteEmptySeriesResponse{}, err
}

func (s *Service) ListFields(
	ctx context.Context,
	req *pb.ListFieldsRequest,
	stream *connect.ServerStream[pb.Field],
) error {
	dataCh, errCh := s.store.ListFields(ctx)
	return streamRecords[pb.Field](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListRuns(
	ctx context.Context,
	req *pb.ListRunsRequest,
	stream *connect.ServerStream[pb.RunId],
) error {
	var err error
	attrFilters := make([]*AttributeFilterValue, len(req.AttributeFilters))
	for i, filter := range req.GetAttributeFilters() {
		attrFilters[i], err = NewAttributeFilterValue(filter)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "AttributeFilter invalid: %v", err)
		}
	}
	tagFilter := NewTagFilterValue(req.GetTagFilter())
	var minStartedAt, maxStartedAt *time.Time
	if req.MinStartedAt != nil {
		t := req.MinStartedAt.AsTime()
		minStartedAt = &t
	}
	if req.MaxStartedAt != nil {
		t := req.MaxStartedAt.AsTime()
		maxStartedAt = &t
	}
	dataCh, errCh := s.store.ListRuns(ctx, attrFilters, tagFilter, minStartedAt, maxStartedAt)
	return streamRecords[pb.RunId](ctx, *stream, dataCh, errCh)
}

func (s *Service) QueryRunData(
	ctx context.Context,
	req *pb.QueryRunDataRequest,
	stream *connect.ServerStream[pb.ChunkData],
) error {
	attrHandles, err := parseUUIDs(req.AttrHandles, "AttrHandle")
	if err != nil {
		return err
	}
	coordHandles, err := parseUUIDs(req.CoordHandles, "CoordHandle")
	if err != nil {
		return err
	}
	attrFilters := make([]*AttributeFilterValue, len(req.AttributeFilters))
	for i, filter := range req.GetAttributeFilters() {
		attrFilters[i], err = NewAttributeFilterValue(filter)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "AttributeFilter invalid: %v", err)
		}
	}
	tagFilter := NewTagFilterValue(req.GetTagFilter())
	var minStartedAt, maxStartedAt *time.Time
	if req.MinStartedAt != nil {
		t := req.MinStartedAt.AsTime()
		minStartedAt = &t
	}
	if req.MaxStartedAt != nil {
		t := req.MaxStartedAt.AsTime()
		maxStartedAt = &t
	}
	dataCh, errCh := s.store.QueryRunData(
		ctx, attrHandles, coordHandles, attrFilters, tagFilter, minStartedAt, maxStartedAt,
	)
	return streamRecords[pb.ChunkData](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListCommonAttributes(
	ctx context.Context,
	req *pb.ListCommonAttributesRequest,
	stream *connect.ServerStream[pb.Field],
) error {
	runHandles, err := parseUUIDs(req.RunHandles, "RunHandle")
	if err != nil {
		return err
	}
	dataCh, errCh := s.store.ListCommonAttributes(ctx, runHandles)
	return streamRecords[pb.Field](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListCommonSeries(
	ctx context.Context,
	req *pb.ListCommonSeriesRequest,
	stream *connect.ServerStream[pb.Series],
) error {
	runHandles, err := parseUUIDs(req.RunHandles, "RunHandle")
	if err != nil {
		return err
	}
	dataCh, errCh := s.store.ListCommonSeries(ctx, runHandles)
	return streamRecords[pb.Series](ctx, *stream, dataCh, errCh)
}
