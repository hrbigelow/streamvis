package util

import (
	"bufio"
	"bytes"
	pb "data-server/pb/data"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	"golang.org/x/sys/unix"

	"google.golang.org/protobuf/proto"
)

func GetLogHandle(path string, mode int) *os.File {
	fh, err := os.OpenFile(path, mode, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return fh
}

func IndexFile(path string) string {
	return fmt.Sprintf("%s.idx", path)
}

func DataFile(path string) string {
	return fmt.Sprintf("%s.log", path)
}

func WriteDelimited(buf *bytes.Buffer, m *pb.Stored) (int, error) {
	// populate buf with m, prepending it with length of message
	// return number of bytes written
	b, err := proto.Marshal(m)
	if err != nil {
		return 0, err
	}

	var lb [10]byte
	n := binary.PutUvarint(lb[:], uint64(len(b)))
	if _, err := buf.Write(lb[:n]); err != nil {
		return 0, err
	}

	nbytes, _ := buf.Write(b)
	return nbytes, nil
}

func ReadDelimited(r *bufio.Reader, m *pb.Stored, max int) (bool, error) {
	// populate message m from buffer r
	n, err := binary.ReadUvarint(r)
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if max > 0 && n > uint64(max) {
		return false, io.ErrUnexpectedEOF
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return false, err
	}
	return true, proto.Unmarshal(buf, m)
}

func SafeWrite(f *os.File, buf *bytes.Buffer) (int64, error) {
	fd := int(f.Fd())

	if err := unix.Flock(fd, unix.LOCK_EX); err != nil {
		return 0, fmt.Errorf("flock(LOCK_EX): %w", err)
	}
	defer unix.Flock(fd, unix.LOCK_UN)

	if _, err := buf.WriteTo(f); err != nil {
		return 0, fmt.Errorf("drain buffer -> file: %w", err)
	}

	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("fsync: %w", err)
	}

	off, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("seek current: %w", err)
	}
	return off, nil
}

func WrapStored(v proto.Message) (*pb.Stored, error) {
	switch x := v.(type) {
	case *pb.Scope:
		return &pb.Stored{Value: &pb.Stored_Scope{Scope: x}}, nil
	case *pb.Name:
		return &pb.Stored{Value: &pb.Stored_Name{Name: x}}, nil
	case *pb.Control:
		return &pb.Stored{Value: &pb.Stored_Control{Control: x}}, nil
	case *pb.DataEntry:
		return &pb.Stored{Value: &pb.Stored_DataEntry{DataEntry: x}}, nil
	case *pb.ConfigEntry:
		return &pb.Stored{Value: &pb.Stored_ConfigEntry{ConfigEntry: x}}, nil
	case *pb.Data:
		return &pb.Stored{Value: &pb.Stored_Data{Data: x}}, nil
	case *pb.Config:
		return &pb.Stored{Value: &pb.Stored_Config{Config: x}}, nil
	default:
		return nil, fmt.Errorf("WrapStored: unsupported type: %T", v)
	}
}

func WrapStreamed(v proto.Message) (*pb.Streamed, error) {
	switch x := v.(type) {
	case *pb.RecordResult:
		return &pb.Streamed{Value: &pb.Streamed_RecordResult{RecordResult: x}}, nil
	case *pb.Data:
		return &pb.Streamed{Value: &pb.Streamed_Data{Data: x}}, nil
	case *pb.Name:
		return &pb.Streamed{Value: &pb.Streamed_Name{Name: x}}, nil
	case *Config:
		return &pb.Streamed{Value: &pb.Streamed_Config{Config: x}}, nil
	case *string:
		return &pb.Streamed{Value: &pb.Streamed_Value{Value: x}}, nil
	case *pb.Tag:
		return &pb.Streamed{Value: &pb.Streamed_Tag{Tag: x}}, nil
	default:
		return nil, fmt.Errorf("WrapStreamed: unsupported type: %T", v)
	}
}

func WrapArray[M proto.Message](msgs []M) ([]*pb.Stored, int, error) {
	size := int(0)
	stored := make([]*pb.Stored, len(msgs))
	idx := 0
	for _, m := range msgs {
		s, err := WrapStored(m)
		if err != nil {
			return nil, 0, fmt.Errorf("Couldn't wrap message: %v", err)
		}
		stored[idx] = s
		size += proto.Size(s)
		idx += 1
	}
	return stored, size, nil
}

/*
func PackScope(scopeId uint32, scope string, buf *bytes.Buffer) error {
	timestamp := timestamppb.Now()
	msg := &pb.Stored{
		Value: &pb.Stored_Scope{
			Scope: &pb.Scope{ScopeId: scopeId, Scope: scope, Time: timestamp},
		},
	}
	return WriteDelimited(buf, msg)
}

func PackDeleteScope(scope string, buf *bytes.Buffer) error {
	msg := &pb.Stored{
		Value: &pb.Stored_Control{
			Control: &pb.Control{Scope: scope, Name: "", Action: pb.Action_DELETE_SCOPE},
		},
	}
	return WriteDelimited(buf, msg)
}

func PackDeleteName(scope string, name string, buf *bytes.Buffer) error {
	msg := &pb.Stored{
		Value: &pb.Stored_Control{
			Control: &pb.Control{Scope: scope, Name: name, Action: pb.Action_DELETE_NAME},
		},
	}
	return WriteDelimited(buf, msg)
}

func PackConfigEntry(entryId uint32, scopeId uint32, begOffset uint64, endOffset uint64, buf *bytes.Buffer) error {
	msg := &pb.Stored{
		Value: &pb.Stored_ConfigEntry{
			ConfigEntry: &pb.ConfigEntry{
				EntryId:   entryId,
				ScopeId:   scopeId,
				BegOffset: begOffset,
				EndOffset: endOffset,
			},
		},
	}
	return WriteDelimited(buf, msg)
}
*/
