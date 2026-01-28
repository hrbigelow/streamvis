package service

import (
	"context"

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

func (s *Service) AddRunTag(
	ctx context.Context,
	req *pb.AddRunTagRequest,
) (*pb.AddRunTagResponse, error) {
	runHandle, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "runHandle invalid UUID: %v", err)
	}
	err = s.store.AddRunTag(ctx, runHandle, req.Tag)
	return &pb.AddRunTagResponse{}, err
}

func (s *Service) DeleteRunTag(
	ctx context.Context,
	req *pb.DeleteRunTagRequest,
) (*pb.DeleteRunTagResponse, error) {
	runHandle, err := uuid.Parse(req.GetRunHandle())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "runHandle invalid UUID: %v", err)
	}
	err = s.store.DeleteRunTag(ctx, runHandle, req.Tag)
	return &pb.DeleteRunTagResponse{}, err
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
	stream *connect.ServerStream[pb.Run],
) error {
	runFilter, err := NewRunFilter(req.GetRunFilter())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "RunFilter invalid: %v", err)
	}
	dataCh, errCh := s.store.ListRuns(ctx, runFilter)
	return streamRecords[pb.Run](ctx, *stream, dataCh, errCh)
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
	runFilter, err := NewRunFilter(req.GetRunFilter())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "RunFilter invalid: %v", err)
	}
	dataCh, errCh := s.store.QueryRunData(ctx, attrHandles, coordHandles, runFilter)
	return streamRecords[pb.ChunkData](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListCommonAttributes(
	ctx context.Context,
	req *pb.ListCommonAttributesRequest,
	stream *connect.ServerStream[pb.Field],
) error {
	runFilter, err := NewRunFilter(req.GetRunFilter())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "RunFilter invalid: %v", err)
	}
	dataCh, errCh := s.store.ListCommonAttributes(ctx, runFilter)
	return streamRecords[pb.Field](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListCommonSeries(
	ctx context.Context,
	req *pb.ListCommonSeriesRequest,
	stream *connect.ServerStream[pb.Series],
) error {
	runFilter, err := NewRunFilter(req.GetRunFilter())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "RunFilter invalid: %v", err)
	}
	dataCh, errCh := s.store.ListCommonSeries(ctx, runFilter)
	return streamRecords[pb.Series](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListStartedAt(
	ctx context.Context,
	req *pb.ListStartedAtRequest,
	stream *connect.ServerStream[pb.RunStartTime],
) error {
	dataCh, errCh := s.store.ListStartedAt(ctx)
	return streamRecords[pb.RunStartTime](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListTags(
	ctx context.Context,
	req *pb.ListTagsRequest,
	stream *connect.ServerStream[pb.TagValue],
) error {
	dataCh, errCh := s.store.ListTags(ctx)
	return streamRecords[pb.TagValue](ctx, *stream, dataCh, errCh)
}

func (s *Service) ListAttributeValues(
	ctx context.Context,
	req *pb.ListAttributeValuesRequest,
	stream *connect.ServerStream[pb.AttributeValues],
) error {
	dataCh, errCh := s.store.ListAttributeValues(ctx)
	return streamRecords[pb.AttributeValues](ctx, *stream, dataCh, errCh)
}
