package index

/* Implements the Store interface using a file-backed index
 */

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"regexp"

	pb "data-server/pb/data"
	"data-server/util"

	"google.golang.org/protobuf/proto"
)

type IndexStore struct {
	index         Index
	appendDataFh  *os.File
	readDataFh    *os.File
	appendIndexFh *os.File
	readIndexFh   *os.File
}

// var _ service.Store = (*IndexStore)(nil)

func New(path string) IndexStore {
	indexPath := util.IndexFile(path)
	dataPath := util.DataFile(path)
	index := NewIndex()

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

func (s *IndexStore) getEntryRecordResult(
	entries []*pb.DataEntry,
) pb.RecordResult {
	res := pb.RecordResult{
		Scopes: make(map[uint32]*pb.Scope),
		Names:  make(map[uint32]*pb.Name),
	}
	maxEndOffset := uint64(0)
	for _, entry := range entries {
		if entry.EndOffset > maxEndOffset {
			maxEndOffset = entry.EndOffset
		}
		if _, ok := res.Names[entry.NameId]; !ok {
			name := s.index.names[entry.NameId]
			res.Names[entry.NameId] = &name
			if _, ok2 := res.Scopes[name.ScopeId]; !ok2 {
				scope := s.index.scopes[name.ScopeId]
				res.Scopes[name.ScopeId] = &scope
			}
		}
	}
	res.FileOffset = maxEndOffset
	return res
}

func (s *IndexStore) GetData(
	scopePat, namePat *regexp.Regexp,
	minOffset uint64,
	ctx context.Context,
) (pb.RecordResult, <-chan *pb.Data, <-chan error) {
	newMsg := func() *pb.Data { return &pb.Data{} }
	entries := s.index.EntryList(scopePat, namePat, minOffset)
	dataCh, errCh := LoadMessages[*pb.DataEntry, *pb.Data](s.readDataFh, entries, ctx, newMsg)
	recordResult := s.getEntryRecordResult(entries)
	return recordResult, dataCh, errCh
}

// tabulates the RecordResult for the list of ConfigEntry objects.
// leaves Names and FileOffset uninitialized
func (s *IndexStore) getConfigEntryRecordResult(
	configEntries []*pb.ConfigEntry,
) pb.RecordResult {
	res := pb.RecordResult{
		Scopes: make(map[uint32]*pb.Scope),
	}
	for _, entry := range configEntries {
		if _, ok := res.Scopes[entry.ScopeId]; !ok {
			scope := s.index.scopes[entry.ScopeId]
			res.Scopes[entry.ScopeId] = &scope
		}
	}
	return res
}

// Implementation
func (s *IndexStore) GetConfigs(
	scopePat *regexp.Regexp,
	ctx context.Context,
) (pb.RecordResult, <-chan *pb.Config, <-chan error) {
	entries := s.index.ConfigEntryList(scopePat, 0)
	result := s.getConfigEntryRecordResult(entries)
	getConfig := func() *pb.Config { return &pb.Config{} }
	dataCh, errCh := LoadMessages[*pb.ConfigEntry, *pb.Config](
		s.readDataFh, entries, ctx, getConfig,
	)
	return result, dataCh, errCh
}

func (s *IndexStore) Add(msg proto.Message) {
	// adds a message to the index store
}

func (s *IndexStore) AddNames(names []*pb.Name) error {
	stored := make([]*pb.Stored, len(names))
	for _, name := range names {
		s.index.names[name.NameId] = *name
	}
	stored, size := util.WrapArray[*pb.Name](names)
	bbuf := bytes.NewBuffer(make([]byte, size))
	for _, msg := range stored {
		if _, err := util.WriteDelimited(bbuf, msg); err != nil {
			return fmt.Errorf("Couldn't write name: %v", err)
		}
	}
	if _, err := util.SafeWrite(s.appendIndexFh, bbuf); err != nil {
		return fmt.Errorf("Couldn't SafeWrite: %v", err)
	}
	return nil
}

func (s *IndexStore) AddDatas(datas []*pb.Data) error {
	stored, size := util.WrapArray[*pb.Data](datas)
	msgSizes := make([]uint64, len(stored))
	totalSize := int64(0)
	bbuf := bytes.NewBuffer(make([]byte, size))
	for i, msg := range stored {
		sz, err := util.WriteDelimited(bbuf, msg)
		if err != nil {
			return fmt.Errorf("Couldn't write name: %v", err)
		}
		msgSizes[i] = uint64(sz)
		totalSize += int64(sz)
	}
	off, err := util.SafeWrite(s.appendDataFh, bbuf)
	if err != nil {
		return fmt.Errorf("Couldn't SafeWrite to data file: %v", err)
	}
	pos := uint64(off - totalSize)
	entries := make([]*pb.DataEntry, len(datas))
	for i, data := range datas {
		entry := &pb.DataEntry{
			EntryId:   data.EntryId,
			NameId:    data.NameId,
			BegOffset: pos,
			EndOffset: pos + msgSizes[i],
		}
		s.index.entries[entry.EntryId] = *entry
		entries[i] = entry
		pos += msgSizes[i]
	}
	storedEntries, storedSize := util.WrapArray[*pb.DataEntry](entries)
	bbuf = bytes.NewBuffer(make([]byte, storedSize))
	for _, msg := range storedEntries {
		if _, err := util.WriteDelimited(bbuf, msg); err != nil {
			return fmt.Errorf("Couldn't write entry: %v", err)
		}
	}
	if _, err := util.SafeWrite(s.appendIndexFh, bbuf); err != nil {
		return fmt.Errorf("Couldn't SafeWrite: %v", err)
	}
	return nil
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
