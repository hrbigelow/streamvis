package index

import (
	"context"
	"log"
	"os"
	"regexp"

	pb "data-server/pb/data"
	"data-server/service"
	"data-server/util"
)

type IndexStore struct {
	index         util.Index
	appendDataFh  *os.File
	readDataFh    *os.File
	appendIndexFh *os.File
	readIndexFh   *os.File
}

var _ service.Store = (*IndexStore)(nil)

func New(path string) IndexStore {
	indexPath := util.IndexFile(path)
	dataPath := util.DataFile(path)
	index := util.Index{}
	if err := index.Load(indexPath); err != nil {
		log.Fatal(err)
	}

	return IndexStore{
		index:         index,
		appendIndexFh: util.GetLogHandle(indexPath, os.O_WRONLY|os.O_APPEND),
		readIndexFh:   util.GetLogHandle(indexPath, os.O_RDONLY),
		appendDataFh:  util.GetLogHandle(dataPath, os.O_WRONLY|os.O_APPEND),
		readDataFh:    util.GetLogHandle(dataPath, os.O_RDONLY),
	}
}

// write a RecordStore method to return a channel with pb.Data
func (s *IndexStore) GetData(
	scopePat, namePat *regexp.Regexp,
	minOffset uint64,
	ctx context.Context,
) (<-chan *pb.Data, <-chan error) {
	entries := s.index.EntryList(scopePat, namePat, minOffset)
	return util.LoadMessages[*pb.DataEntry, *pb.Data](
        s.readDataFh, entries, ctx, func() *pb.Data { return &pb.Data{} }
    )
}

func (s *IndexStore) GetConfigs(
	scopePat *regexp.Regexp,
	minOffset uint64,
	ctx context.Context,
) (<-chan *pb.Config, <-chan error) {
	entries := s.index.ConfigEntryList(scopePat, minOffset)
    getConfig := func() *pb.Config { return &pb.Config{} }
    return util.LoadMessages[*pb.ConfigEntry, *pb.Config](
        s.readDataFh, entries, ctx, getConfig 
    )
}


func (s *IndexStore) GetMaxId() uint32 {
	return s.index.MaxId()
}

func (s *IndexStore) GetScopes(scopePat *regexp.Regexp) []string {
	return s.index.ScopeList(scopePat)
}

func (s *IndexStore) GetNames(scopePat, namePat *regexp.Regexp) [][2]string {
	return s.index.NameList(scopePat, namePat)
}
