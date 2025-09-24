package svc

import (
	"context"
	"log"
	"os"
	"regexp"

	pb "data-server/pb/data"

	"data-server/util"
)


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
