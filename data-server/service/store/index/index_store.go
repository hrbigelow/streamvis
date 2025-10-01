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
	unwrap := func(s *pb.Stored) *pb.Data { return s.Value.(*pb.Stored_Data).Data }
	entries := s.index.EntryList(scopePat, namePat, minOffset)
	dataCh, errCh := LoadMessages[*pb.DataEntry, *pb.Data](s.readDataFh, entries, ctx, unwrap)
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
	unwrap := func(sto *pb.Stored) *pb.Config { return sto.Value.(*pb.Stored_Config).Config }
	dataCh, errCh := LoadMessages[*pb.ConfigEntry, *pb.Config](
		s.readDataFh, entries, ctx, unwrap,
	)
	return result, dataCh, errCh
}

func (s *IndexStore) AddScope(scope *pb.Scope) error {
	msg := util.WrapStored(scope)
	s.index.updateWithItem(msg)
	buf := bytes.NewBuffer(make([]byte, 0, proto.Size(msg)+10))
	if _, err := util.WriteDelimited(buf, msg); err != nil {
		panic(fmt.Errorf("Couldn't write name: %v", err))
	}
	if _, err := util.SafeWrite(s.appendIndexFh, buf); err != nil {
		return fmt.Errorf("Couldn't SafeWrite to Index file: %v", err)
	}
	return nil
}

func (s *IndexStore) AddConfig(config *pb.Config) error {
	stored := util.WrapStored(config)
	buf, err := proto.Marshal(stored)
	if err != nil {
		return fmt.Errorf("Couldn't marshal config: %v", err)
	}
	off, err2 := util.SafeWrite(s.appendDataFh, bytes.NewBuffer(buf))
	if err2 != nil {
		return fmt.Errorf("Couldn't SafeWrite to data file: %v", err)
	}
	end := uint64(off)
	beg := uint64(off - int64(len(buf)))
	entry := &pb.ConfigEntry{
		EntryId:   config.EntryId,
		ScopeId:   config.ScopeId,
		BegOffset: beg,
		EndOffset: end,
	}
	stored2 := util.WrapStored(entry)
	s.index.updateWithItem(stored2)

	bbuf := bytes.NewBuffer(make([]byte, 0, proto.Size(stored2)+10))
	if _, err := util.WriteDelimited(bbuf, stored2); err != nil {
		return fmt.Errorf("Couldn't write ConfigEntry: %v", err)
	}
	if _, err := util.SafeWrite(s.appendIndexFh, bbuf); err != nil {
		return fmt.Errorf("Couldn't SafeWrite to Index file: %v", err)
	}
	return nil

}

func (s *IndexStore) AddNames(names []*pb.Name) error {
	stored, size := util.WrapArray[*pb.Name](names)
	bbuf := bytes.NewBuffer(make([]byte, 0, size))
	for _, msg := range stored {
		s.index.updateWithItem(msg)
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
	buf := make([]byte, 0, size)
	for i, msg := range stored {
		msgSize := proto.Size(msg)
		msgSizes[i] = uint64(msgSize)
		var err error
		buf, err = proto.MarshalOptions{}.MarshalAppend(buf, msg)
		if err != nil {
			return fmt.Errorf("Couldn't marshal: %v", err)
		}
	}
	totalSize := int64(len(buf))
	off, err := util.SafeWrite(s.appendDataFh, bytes.NewBuffer(buf))
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
		entries[i] = entry
		pos += msgSizes[i]
	}
	storedEntries, storedSize := util.WrapArray[*pb.DataEntry](entries)
	bbuf := bytes.NewBuffer(make([]byte, 0, storedSize))
	for _, msg := range storedEntries {
		s.index.updateWithItem(msg)
		if _, err := util.WriteDelimited(bbuf, msg); err != nil {
			return fmt.Errorf("Couldn't write entry: %v", err)
		}
	}
	if _, err := util.SafeWrite(s.appendIndexFh, bbuf); err != nil {
		return fmt.Errorf("Couldn't SafeWrite: %v", err)
	}
	return nil
}

func (s *IndexStore) DeleteScopeNames(scope string, names []string) {
	buf := bytes.NewBuffer(make([]byte, 0, 100))
	for _, name := range names {
		ct := &pb.Control{
			Scope:  scope,
			Name:   name,
			Action: pb.Action_DELETE_NAME,
		}
		msg := util.WrapStored(ct)
		s.index.updateWithItem(msg)
		if _, err := util.WriteDelimited(buf, msg); err != nil {
			panic(fmt.Errorf("Couldn't write delimited: %v", err))
		}
	}
	if _, err := util.SafeWrite(s.appendIndexFh, buf); err != nil {
		panic(fmt.Errorf("Couldn't SafeWrite: %v", err))
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
