package util

import (
	"data-server/pb/data"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func getLogHandle(path string, mode int) *os.File {
	fh, err := os.OpenFile(path, mode, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return fh
}

func indexFile(path string) string {
	return fmt.Sprintf("%s.idx", path)
}

func dataFile(path string) string {
	return fmt.Sprintf("%s.log", path)
}

var kindCodes map[reflect.Type]data.StoredType

// var dtypeToProto map[

func init() {
	kindCodes = map[reflect.Type]data.StoredType{
		reflect.TypeOf(&data.Scope{}):       data.StoredType_SCOPE,
		reflect.TypeOf(&data.Name{}):        data.StoredType_NAME,
		reflect.TypeOf(&data.DataEntry{}):   data.StoredType_DATA_ENTRY,
		reflect.TypeOf(&data.ConfigEntry{}): data.StoredType_CONFIG_ENTRY,
		reflect.TypeOf(&data.Data{}):        data.StoredType_DATA,
		reflect.TypeOf(&data.Config{}):      data.StoredType_CONFIG,
		reflect.TypeOf(&data.Control{}):     data.StoredType_CONTROL,
	}
}

func PackMessage(message proto.Message) ([]byte, error) {
	msgType := reflect.TypeOf(message)
	kindCode := kindCodes[msgType]
	content, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}
	lengthCode := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthCode, uint32(len(content)))

	result := make([]byte, 0, 1+4+len(content))
	result = append(result, byte(kindCode))
	result = append(result, lengthCode...)
	result = append(result, content...)

	return result, nil
}

func PackScope(scopeId uint32, scope string) ([]byte, error) {
	timestamp := timestamppb.Now()
	scopeMsg := &data.Scope{ScopeId: scopeId, Scope: scope, Time: timestamp}
	return PackMessage(scopeMsg)
}

func PackDeleteScope(scope string) ([]byte, error) {
	control := &data.Control{Scope: scope, Name: "", Action: data.Action_DELETE_SCOPE}
	return PackMessage(control)
}

func PackDeleteName(scope string, name string) ([]byte, error) {
	control := &data.Control{Scope: scope, Name: name, Action: data.Action_DELETE_NAME}
	return PackMessage(control)
}

func PackConfigEntry(entryId uint32, scopeId uint32, begOffset uint64, endOffset uint64) ([]byte, error) {
	configEntry := &data.ConfigEntry{
		EntryId:   entryId,
		ScopeId:   scopeId,
		BegOffset: begOffset,
		EndOffset: endOffset,
	}
	return PackMessage(configEntry)
}

type Item struct {
	Kind data.StoredType
	Msg  proto.Message
}

type Unpacker struct {
	pack []byte
	off  int
	err  error
	cur  Item
}

func NewUnpacker(pack []byte) *Unpacker {
	return &Unpacker{pack: pack}
}

func (u *Unpacker) Scan() bool {
	if u.err != nil {
		return false
	}
	kind := data.StoredType(u.pack[u.off])
	u.off++
	length := int(binary.BigEndian.Uint32(u.pack[u.off : u.off+4]))
	u.off += 4
	if length < 0 || u.off+length > len(u.pack) {
		u.err = io.ErrUnexpectedEOF
		return false
	}
	payload := u.pack[u.off : u.off+length]
	u.off += length

	var msg proto.Message
	switch kind {
	case data.StoredType_SCOPE:
		msg = &data.Scope{}
	case data.StoredType_NAME:
		msg = &data.Name{}
	case data.StoredType_DATA_ENTRY:
		msg = &data.DataEntry{}
	case data.StoredType_CONFIG_ENTRY:
		msg = &data.ConfigEntry{}
	case data.StoredType_DATA:
		msg = &data.Data{}
	case data.StoredType_CONFIG:
		msg = &data.Config{}
	case data.StoredType_CONTROL:
		msg = &data.Control{}
	}

	if err := proto.Unmarshal(payload, msg); err != nil {
		u.err = err
		return false
	}
	u.cur = Item{Kind: kind, Msg: msg}
	return true
}

func (u *Unpacker) Item() Item    { return u.cur }
func (u *Unpacker) Err() error    { return u.err }
func (u *Unpacker) Consumed() int { return u.off }

type Index struct {
	scopeFilter    *regexp.Regexp
	nameFilters    []*regexp.Regexp
	scopes         map[uint32]data.Scope
	names          map[uint32]data.Name
	entries        map[uint32]data.DataEntry
	configEntries  map[uint32]data.ConfigEntry
	tagToNames     map[[2]string][]uint32
	nameToEntries  map[uint32][]uint32
	scopeToConfigs map[string][]uint32
	fileOffset     uint64
}

func parseRegexps(patterns []string) ([]*regexp.Regexp, error) {
	var regexps []*regexp.Regexp
	var messages []string
	for i, pattern := range patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			err = fmt.Errorf("pattern (%d) (%q): %w", i, pattern, err)
			messages = append(messages, err.Error())
			continue
		}
		regexps = append(regexps, re)
	}
	if len(messages) != 0 {
		return []*regexp.Regexp{}, fmt.Errorf("%s", strings.Join(messages, "\n"))
	}
	return regexps, nil
}

func IndexFromMessage(request data.Index) (Index, error) {
	scopeFilter, err := regexp.Compile(request.ScopeFilter)
	if err != nil {
		return Index{}, fmt.Errorf("failed to parse scopeFilter", err)
	}
	nameFilters, err := parseRegexps(request.NameFilters)
	if err != nil {
		return Index{}, fmt.Errorf("failed to parse nameFilters", err)
	}

	scopes := make(map[uint32]data.Scope, len(request.Scopes))
	for k, v := range request.Scopes {
		if v != nil {
			scopes[k] = *v
		}
	}
	names := make(map[uint32]data.Name, len(request.Names))
	for k, v := range request.Names {
		if v != nil {
			names[k] = *v
		}
	}
	return Index{
		scopeFilter: scopeFilter,
		nameFilters: nameFilters,
		scopes:      scopes,
		names:       names,
		fileOffset:  request.FileOffset,
	}, nil
}

func IndexFromFilters(scopeFilter string, nameFilters []string) (Index, error) {
	scopeFilterRx, err := regexp.Compile(scopeFilter)
	if err != nil {
		return Index{}, fmt.Errorf("failed to parse scopeFilter regexp", err)
	}
	nameFiltersRx, err := parseRegexps(nameFilters)
	if err != nil {
		return Index{}, fmt.Errorf("failed to parse nameFilters", err)
	}
	return Index{
		scopeFilter: scopeFilterRx,
		nameFilters: nameFiltersRx,
		scopes:      make(map[uint32]data.Scope),
		names:       make(map[uint32]data.Name),
	}, nil
}

func (idx *Index) EntryList() []data.DataEntry {
	entries := make([]data.DataEntry, 0, len(idx.entries))
	for _, entry := range idx.entries {
		entries = append(entries, entry)
	}
	return entries
}

func (idx *Index) ConfigEntryList() []data.ConfigEntry {
	entries := make([]data.ConfigEntry, 0, len(idx.configEntries))
	for _, entry := range idx.configEntries {
		entries = append(entries, entry)
	}
	return entries
}

func (idx *Index) ScopeList() []string {
	scopeNames := make(map[string]struct{}, 0)
	for scopeId, scopeMsg := range idx.scopes {
		for _, name := range idx.names {
			if name.ScopeId == scopeId {
				scopeNames[scopeMsg.Scope] = struct{}{}
				break
			}
		}
	}
	scopeList := make([]string, 0, len(scopeNames))
	for scopeName, _ := range scopeNames {
		scopeList = append(scopeList, scopeName)
	}
	return scopeList
}

func (idx *Index) NameList() []string {
	names := make(map[string]struct{}, 0)
	for _, nameMsg := range idx.names {
		names[nameMsg.Name] = struct{}{}
	}
	nameList := make([]string, 0, len(names))
	for name, _ := range names {
		nameList = append(nameList, name)
	}
	return nameList
}

type DataKey struct {
	scopeId uint32
	scope   string
	nameId  uint32
	name    string
	index   uint32
}

func (idx *Index) getKey(data data.Data) DataKey {
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

func (idx *Index) getName(data data.Data) data.Name {
	return idx.names[data.NameId]
}

func (idx *Index) filter(scope *string, name *string) bool {
	if scope != nil && !idx.scopeFilter.MatchString(*scope) {
		return false
	}
	if name != nil {
		for _, nameRx := range idx.nameFilters {
			if nameRx.MatchString(*name) {
				return true
			}
		}
		return false
	}
	return true
}

func (idx *Index) updateWithItem(item Item) {
  switch m := item.Msg.(type) {
  case *data.Scope: {
    if _, ok := idx.scopes[m.ScopeId]; ok { 
      panic(fmt.Sprintf("Duplicate scopeId %s in index", m.ScopeId))
    }
    if idx.filter(&m.Scope, nil) {
      idx.scopes[m.ScopeId] = *m
    }
  }
  case *data.Name: {
    if idx.filter(nil, &m.Name) {
      if _, ok1 := idx.scopes[m.ScopeId]; ok1 {
        if _, ok2 := idx.names[m.NameId]; ok2 {
          panic(fmt.Sprintf("Duplicate nameId %s in index", m.NameId))
        }
        scope := idx.scopes[m.ScopeId].Scope
        tag := [2]string{scope, m.Name}
        names := idx.tagToNames[tag]
        if names == nil {
          names := make([]uint32, 0)
          idx.tagToNames[tag] = names
        }
        idx.tagToNames[tag] = append(idx.tagToNames[tag], m.NameId)
      }
    }
  }
  case *data.Control: {
    if !idx.filter(&m.Scope, &m.Name) {
      return
    }
    if m.Action == data.Action_DELETE_NAME {
      tag := [2]string{m.Scope, m.Name}
      names := idx.tagToNames[tag]
      if names == nil {
        names := make([]uint32, 0)
        idx.tagToNames[tag] = names
      }
      for _, nameId := range names {
        delete(idx.names, nameId)
        // TODO: check for nil
        for _, entryId := range idx.nameToEntries[nameId] {
          delete(idx.entries, entryId)
        }
        delete(idx.nameToEntries, nameId)
      }
      delete(idx.tagToNames, tag)
    }
  }
  case *data.DataEntry: {
    if _, ok := idx.names[m.NameId]; ok {
      idx.entries[m.NameId] = *m 
      entries := idx.nameToEntries[m.NameId]
      if entries == nil {
        entries := make([]uint32, 0)
        idx.nameToEntries[m.NameId] = entries
      }
      idx.nameToEntries[m.NameId] = append(idx.nameToEntries[m.NameId], m.EntryId)
    }
  }

  case *data.ConfigEntry: {
    if scopeMsg, ok := idx.scopes[m.ScopeId]; ok {
      scope := scopeMsg.Scope
      idx.configEntries[m.EntryId] = *m 
      configIds := idx.scopeToConfigs[scope]
      if configIds == nil {
        configIds = make([]uint32, 0)
        idx.scopeToConfigs[scope] = configIds
      }
      idx.scopeToConfigs[scope] = append(idx.scopeToConfigs[scope], m.EntryId)
    }
  }
}
}


func (idx *Index) update(fh *os.File) {
  fh.Seek(int64(idx.fileOffset), 0)
  pack := make([]byte, 0)
  if _, err := fh.Read(pack); err != nil {
    panic(fmt.Errorf("Error reading index file, %w", err.Error()))
  }
  unpacker := NewUnpacker(pack)
  for unpacker.Scan() {
    idx.updateWithItem(unpacker.Item())
  }
  if err := unpacker.Err(); err != nil {
    panic(err)
  }
  idx.fileOffset = uint64(unpacker.Consumed())
}


func (idx *Index) toMessage() data.Index {
  nameFilters := make([]string, len(idx.nameFilters))
  for _, re := range idx.nameFilters {
    nameFilters = append(nameFilters, re.String())
  }
  scopesMap := make(map[uint32]*data.Scope, len(idx.scopes))
  namesMap := make(map[uint32]*data.Name, len(idx.names))

  for k, v := range idx.scopes {
    scopesMap[k] = &v
  }
  for k, v := range idx.names {
    namesMap[k] = &v
  }

  return data.Index{
    ScopeFilter: idx.scopeFilter.String(),
    NameFilters: nameFilters,
    Scopes: scopesMap,
    Names: namesMap,
    FileOffset: idx.fileOffset,
  }
}

func (idx *Index) toBytes() []byte {
}


