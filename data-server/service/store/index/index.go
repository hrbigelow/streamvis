package index

/* Implements the Store interface using a file-backed index
 */

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"

	pb "data-server/pb/data"
	"data-server/service"
	"data-server/util"

	"google.golang.org/protobuf/proto"
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
	ptrs := make([]*pb.DataEntry, len(entries))
	for i := range entries {
		ptrs[i] = &entries[i]
	}
	newMsg := func() *pb.Data { return &pb.Data{} }
	return util.LoadMessages[*pb.DataEntry, *pb.Data](
		s.readDataFh, ptrs, ctx, newMsg,
	)
}

func (s *IndexStore) GetConfigs(
	scopePat *regexp.Regexp,
	minOffset uint64,
	ctx context.Context,
) (<-chan *pb.Config, <-chan error) {
	entries := s.index.ConfigEntryList(scopePat, minOffset)
	ptrs := make([]*pb.ConfigEntry, len(entries))
	for i := range entries {
		ptrs[i] = &entries[i]
	}
	getConfig := func() *pb.Config { return &pb.Config{} }
	return util.LoadMessages[*pb.ConfigEntry, *pb.Config](
		s.readDataFh, ptrs, ctx, getConfig,
	)
}

func (s *IndexStore) Add(msg proto.Message) {
	// adds a message to the index store
}

func (s *IndexStore) AddNames(names []*pb.Name) error {
	// adds the list of names to the index store
    size := int64(0)
    stored := make([]*pb.Stored, len(names))
    idx := 0
	for _, name := range names {
		s.index.names[name.NameId] = *name
    }
    stored, size, err := util.WrapArray[*pb.Name](names)
    if err != nil {
        return fmt.Errorf("Couldn't wrap messages: %v", err)
    }
    bbuf := bytes.NewBuffer(make([]byte, size))
    for i, msg := range stored {
		if _, err := util.WriteDelimited(bbuf, msg); err != nil {
			return fmt.Errorf("Couldn't write name: %v", err)
		}
	}
    if _, err := util.SafeWrite(s.appendIndexFh, bbuf) {
        return fmt.Errorf("Couldn't SafeWrite: %v", err)
    }
    return nil
}

func (s *IndexStore) AddData(datas []*pb.Data) error {
    stored, size, err := util.WrapArray[*pb.Data](datas)
    if err != nil {
        return fmt.Errorf("Couldn't wrap messages: %v", err)
    }
    msgSizes := make([]int, len(stored))
    totalSize := int64(0) 
    bbuf := bytes.NewBuffer(make([]byte, size))
    for i, msg := range stored {
		if sz, err := util.WriteDelimited(bbuf, msg); err != nil {
			return fmt.Errorf("Couldn't write name: %v", err)
		}
        msgSizes[i] = sz
        totalSize += sz
    }
    if off, err := util.SafeWrite(s.appendDataFh, bbuf) {
        return fmt.Errorf("Couldn't SafeWrite to data file: %v", err)
    }
    pos := off - totalSize 
    entries := make([]pb.DataEntry, len(datas))
    for i, data := range datas {
        entry := pb.DataEntry{
            EntryId: data.EntryId,
            NameId: data.NameId,
            BegOffset: pos,
            EndOffset: pos + msgSizes[i],
        }
        s.index.entries[entry.EntryId] = entry
        pos += msgSizes[i]
    }
    storedEntries, storedSize, err := util.WrapArray[pb.DataEntry](entries)
    if err != nil {
        return fmt.Errorf("Couldn't wrap DataEntry messages: %v", err)
    }
    bbuf = bytes.NewBuffer(make([]byte, storedSize))
    for _, msg := range storedEntries {
        if _, err := util.WriteDelimited(bbuf, msg); err != nil {
			return fmt.Errorf("Couldn't write entry: %v", err)
		}
    }
    if _, err := util.SafeWrite(s.appendIndexFh, bbuf); err != nil {
        return fmt.Errorf("Couldn't SafeWrite: %v", err)
    }
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
