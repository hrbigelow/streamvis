package index

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"iter"
	"maps"
	"os"
	"regexp"
	"slices"

	pb "data-server/pb/streamvis/v1"
	"data-server/util"

	"google.golang.org/protobuf/proto"
)

type Unpacker struct {
	reader *bufio.Reader
	err    error
	cur    *pb.Stored
}

func NewUnpacker(file *os.File) *Unpacker {
	reader := bufio.NewReader(file)
	return &Unpacker{
		reader: reader,
		cur:    &pb.Stored{},
	}
}

func (u *Unpacker) Scan() bool {
	if u.err != nil {
		return false
	}
	ok, err := util.ReadDelimited(u.reader, u.cur, 0)
	if err != nil {
		u.err = err
		return false
	}
	if !ok {
		return false
	}
	return true
}

func (u *Unpacker) Item() *pb.Stored { return u.cur }
func (u *Unpacker) Err() error       { return u.err }

/*
Foreign Keys:

Scope               -> nil
Name.ScopeId        -> Scope
DataEntry.NameId    -> Name
Data.EntryId        -> DataEntry
ConfigEntry.ScopeId -> Scope
Config.EntryId      -> ConfigEntry

*/

type Index struct {
	scopes         map[uint32]pb.Scope
	names          map[uint32]pb.Name
	entries        map[uint32]pb.DataEntry
	configEntries  map[uint32]pb.ConfigEntry
	tagToNames     map[[2]string][]uint32
	nameToEntries  map[uint32][]uint32
	scopeToConfigs map[string][]uint32
}

func NewIndex() Index {
	return Index{
		scopes:         make(map[uint32]pb.Scope),
		names:          make(map[uint32]pb.Name),
		entries:        make(map[uint32]pb.DataEntry),
		configEntries:  make(map[uint32]pb.ConfigEntry),
		tagToNames:     make(map[[2]string][]uint32),
		nameToEntries:  make(map[uint32][]uint32),
		scopeToConfigs: make(map[string][]uint32),
	}
}

func (idx *Index) EntryList(scopePat, namePat *regexp.Regexp, minOffset uint64) []*pb.DataEntry {
	entries := make([]*pb.DataEntry, 0, 10)
	for _, entry := range idx.entries {
		if entry.EndOffset <= minOffset {
			continue
		}
		name := idx.names[entry.NameId]
		if !namePat.MatchString(name.Name) {
			continue
		}
		scope := idx.scopes[name.ScopeId]
		if !scopePat.MatchString(scope.Scope) {
			continue
		}
		ptr := new(pb.DataEntry)
		*ptr = entry
		entries = append(entries, ptr)
	}
	return entries
}

func (idx *Index) ConfigEntryList(scopePat *regexp.Regexp, minOffset uint64) []*pb.ConfigEntry {
	entries := make([]*pb.ConfigEntry, 0, 10)
	for _, entry := range idx.configEntries {
		if entry.EndOffset <= minOffset {
			continue
		}
		scope := idx.scopes[entry.ScopeId]
		if !scopePat.MatchString(scope.Scope) {
			continue
		}
		ptr := new(pb.ConfigEntry)
		*ptr = entry
		entries = append(entries, ptr)
	}
	return entries
}

func (idx *Index) GetScopes(scopePat *regexp.Regexp) map[uint32]*pb.Scope {
	// return a list of pb.Scope objects having content and matching scopePat
	scopes := make(map[uint32]*pb.Scope)
	for scopeId, scope := range idx.scopes {
		if !scopePat.MatchString(scope.Scope) {
			continue
		}
		for _, name := range idx.names {
			if name.ScopeId == scopeId {
				scopes[scopeId] = &scope
				break
			}
		}
	}
	return scopes
}

func (idx *Index) GetNames(scopePat, namePat *regexp.Regexp) map[uint32]*pb.Name {
	names := make(map[uint32]*pb.Name)
	for scopeId, scope := range idx.scopes {
		if !scopePat.MatchString(scope.Scope) {
			continue
		}
		for _, name := range idx.names {
			if name.ScopeId != scopeId {
				continue
			}
			if !namePat.MatchString(name.Name) {
				continue
			}
			names[name.NameId] = &name
		}
	}
	return names
}

type DataKey struct {
	scopeId uint32
	scope   string
	nameId  uint32
	name    string
	index   uint32
}

func (idx *Index) getKey(data pb.Data) DataKey {
	name := idx.names[data.NameId]
	scope := idx.scopes[name.ScopeId]
	return DataKey{
		scopeId: scope.ScopeId,
		scope:   scope.Scope,
		nameId:  name.NameId,
		name:    name.Name,
		index:   data.Index,
	}
}

func (idx *Index) getName(data pb.Data) pb.Name {
	return idx.names[data.NameId]
}

// updates the index state with the stored item
func (idx *Index) updateWithItem(item *pb.Stored) {
	switch m := item.GetValue().(type) {
	case *pb.Stored_Scope:
		sc := m.Scope
		if _, ok := idx.scopes[sc.ScopeId]; ok {
			panic(fmt.Sprintf("Duplicate scopeId %s in index", sc.ScopeId))
		}
		idx.scopes[sc.ScopeId] = *sc
	case *pb.Stored_Name:
		nm := m.Name
		if _, ok1 := idx.scopes[nm.ScopeId]; ok1 {
			if _, ok2 := idx.names[nm.NameId]; ok2 {
				panic(fmt.Sprintf("Duplicate nameId %s in index", nm.NameId))
			}
			scope := idx.scopes[nm.ScopeId].Scope
			idx.names[nm.NameId] = *nm

			tag := [2]string{scope, nm.Name}
			names := idx.tagToNames[tag]
			if names == nil {
				names := make([]uint32, 0)
				idx.tagToNames[tag] = names
			}
			idx.tagToNames[tag] = append(idx.tagToNames[tag], nm.NameId)
		}
	case *pb.Stored_Control:
		ct := m.Control
		if ct.Action == pb.Action_ACTION_DELETE_NAME {
			tag := [2]string{ct.Scope, ct.Name}
			names := idx.tagToNames[tag]
			if names == nil {
				names := make([]uint32, 0)
				idx.tagToNames[tag] = names
			}
			for _, nameId := range names {
				delete(idx.names, nameId)
				if ne, ok := idx.nameToEntries[nameId]; ok {
					for _, entryId := range ne {
						delete(idx.entries, entryId)
					}
					delete(idx.nameToEntries, nameId)
				} else {
					// may fail if AddNames was called but not AddDatas (due to
					// interrupted client)
				}
			}
			delete(idx.tagToNames, tag)
		}
	case *pb.Stored_DataEntry:
		de := m.DataEntry
		if _, ok := idx.names[de.NameId]; ok {
			idx.entries[de.EntryId] = *de
			entries := idx.nameToEntries[de.NameId]
			if entries == nil {
				entries := make([]uint32, 0)
				idx.nameToEntries[de.NameId] = entries
			}
			idx.nameToEntries[de.NameId] = append(idx.nameToEntries[de.NameId], de.EntryId)
		}

	case *pb.Stored_ConfigEntry:
		ce := m.ConfigEntry
		if scopeMsg, ok := idx.scopes[ce.ScopeId]; ok {
			scope := scopeMsg.Scope
			idx.configEntries[ce.EntryId] = *ce
			configIds := idx.scopeToConfigs[scope]
			if configIds == nil {
				configIds = make([]uint32, 0)
				idx.scopeToConfigs[scope] = configIds
			}
			idx.scopeToConfigs[scope] = append(idx.scopeToConfigs[scope], ce.EntryId)
		}
	default:
		panic(fmt.Errorf("updateWithItem: unsupported type: %T", item))

	}
}

func (idx *Index) Load(indexPath string) error {
	fh, err := os.Open(indexPath)
	defer fh.Close()
	if err != nil {
		return fmt.Errorf("Error opening index file: %w", err)
	}
	unpacker := NewUnpacker(fh)
	for unpacker.Scan() {
		idx.updateWithItem(unpacker.Item())
	}
	if err := unpacker.Err(); err != nil {
		return fmt.Errorf("Error unpacking index file: %w", err)
	}
	return nil
}

func maxSeq[T cmp.Ordered](s iter.Seq[T]) (T, bool) {
	var max T
	ok := false
	for v := range s { // lazily consumes the sequence
		if !ok || v > max {
			max, ok = v, true
		}
	}
	return max, ok
}

func (idx *Index) MaxId() uint32 {
	maxId := uint32(0)
	updateMax := func(keys iter.Seq[uint32]) {
		if i, ok := maxSeq(keys); ok {
			if i > maxId {
				maxId = i
			}
		}
	}
	updateMax(maps.Keys(idx.scopes))
	updateMax(maps.Keys(idx.names))
	updateMax(maps.Keys(idx.entries))
	updateMax(maps.Keys(idx.configEntries))
	return maxId
}

type offsets interface {
	GetBegOffset() uint64
	GetEndOffset() uint64
}

func LoadMessages[E offsets, M proto.Message](
	fh *os.File,
	entries []E, // E is pointer type
	ctx context.Context,
	unwrap func(*pb.Stored) M,
) (<-chan M, <-chan error) {
	slices.SortFunc(entries, func(a, b E) int {
		return cmp.Compare(a.GetBegOffset(), b.GetBegOffset())
	})
	out := make(chan M, 64)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for _, e := range entries {
			select {
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			default:
			}

			beg, end := e.GetBegOffset(), e.GetEndOffset()
			if end < beg {
				errc <- fmt.Errorf("bad offsets: beg=%d end=%d", beg, end)
				return
			}
			length := end - beg
			buf := make([]byte, int(length))

			if _, err := fh.ReadAt(buf, int64(beg)); err != nil {
				errc <- fmt.Errorf("readAt failed at %d (%d bytes): %w", beg, length, err)
				return
			}
			stored := &pb.Stored{}

			if err := proto.Unmarshal(buf, stored); err != nil {
				errc <- fmt.Errorf("unmarshal: %w", err)
				return
			}
			msg := unwrap(stored)

			select {
			case out <- msg:
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			}
		}
	}()
	return out, errc
}
