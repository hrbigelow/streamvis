package svc

import (
	"context"
	"log"
	"os"
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "data-server/pb/data"

	"data-server/util"
)

type RecordService struct {
    pb pb.UnimplementedRecordServiceServer
    store RecordStore
    lastIssuedId uint32 
}

type RecordStore interface { 
    getData(
        scopePat, namePat *regexp.Regexp, 
        minOffset uint64,
        ctx context.Context,
    ) (<-chan *pb.Data, <-chan error)
}


type IndexedRecordStore struct {
    index util.Index
    appendDataFh *os.File
    readDataFh *os.File
    appendIndexFh *os.File
    readIndexFh *os.File
}

func NewIndexedRecordStore(path string) IndexedRecordStore {
    indexPath := util.IndexFile(path)
    dataPath := util.DataFile(path)
    index := util.Index{}
    if err := index.Load(indexPath); err != nil {
        log.Fatal(err)
    }

    return IndexedRecordStore{
        index: index,
        appendIndexFh: util.GetLogHandle(indexPath, os.O_WRONLY | os.O_APPEND),
        readIndexFh: util.GetLogHandle(indexPath, os.O_RDONLY),
        appendDataFh: util.GetLogHandle(dataPath, os.O_WRONLY | os.O_APPEND),
        readDataFh: util.GetLogHandle(dataPath, os.O_RDONLY),
    }
}

// write a RecordStore method to return a channel with pb.Data 
func (s *IndexedRecordStore) getData(
    scopePat, namePat *regexp.Regexp, 
    minOffset uint64,
    ctx context.Context,
) (<-chan *pb.Data, <-chan error) {
    entries := s.index.EntryList(scopePat, namePat, minOffset)
    return util.LoadData(s.readDataFh, entries, ctx)
}


func (s *IndexedRecordStore) getMaxId() uint32 {
    return s.index.MaxId()
}

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

