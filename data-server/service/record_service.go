package recordsvc


type RecordStore interface { 
    getData(
        scopePat, namePat *regexp.Regexp, 
        minOffset uint64,
        ctx context.Context,
    ) (<-chan *pb.Data, <-chan error)
}


type RecordService struct {
    pb pb.UnimplementedRecordServiceServer
    store RecordStore
    lastIssuedId uint32 
}

func New() *RecordService {


func (s *RecordService) issueId() (uint32, error) {
    s.lastIssuedId += 1
    return s.lastIssuedId, nil
}

func (s *RecordService) QueryRecords(
    req *pb.RecordRequest, 
    stream pb.RecordService_QueryRecordsServer,
) error {
    scopePat, err := regexp.Compile(req.GetScopePattern())
    if err != nil {
        return status.Errorf(codes.InvalidArgument, "bad scope_regex: %v", err)
    }
    namePat, err := regexp.Compile(req.GetNamePattern())
    if err != nil {
        return status.Errorf(codes.InvalidArgument, "bad name_regex: %v", err)
    }
    ctx := stream.Context()

    dataCh, errCh := s.store.getData(scopePat, namePat, req.GetFileOffset(), ctx)

    for {
        select {
        case <- ctx.Done():
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
            msg := &pb.StreamedRecord{
                Record: &pb.StreamedRecord_Data{Data: d},
            }

            if err := stream.Send(msg); err != nil {
                return status.Errorf(codes.Unavailable, "send failed: %v", err)
            }
        }
    }
}

