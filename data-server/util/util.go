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

func WrapStored(v proto.Message) *pb.Stored {
	switch x := v.(type) {
	case *pb.Scope:
		return &pb.Stored{Value: &pb.Stored_Scope{Scope: x}}
	case *pb.Name:
		return &pb.Stored{Value: &pb.Stored_Name{Name: x}}
	case *pb.Control:
		return &pb.Stored{Value: &pb.Stored_Control{Control: x}}
	case *pb.DataEntry:
		return &pb.Stored{Value: &pb.Stored_DataEntry{DataEntry: x}}
	case *pb.ConfigEntry:
		return &pb.Stored{Value: &pb.Stored_ConfigEntry{ConfigEntry: x}}
	case *pb.Data:
		return &pb.Stored{Value: &pb.Stored_Data{Data: x}}
	case *pb.Config:
		return &pb.Stored{Value: &pb.Stored_Config{Config: x}}
	default:
		panic(fmt.Errorf("WrapStored: unsupported type: %T", v))
	}
}

func WrapStreamed(v proto.Message) *pb.Streamed {
	switch x := v.(type) {
	case *pb.RecordResult:
		return &pb.Streamed{Value: &pb.Streamed_Index{Index: x}}
	case *pb.Data:
		return &pb.Streamed{Value: &pb.Streamed_Data{Data: x}}
	case *pb.Name:
		return &pb.Streamed{Value: &pb.Streamed_Name{Name: x}}
	case *pb.Config:
		return &pb.Streamed{Value: &pb.Streamed_Config{Config: x}}
	case *pb.Tag:
		return &pb.Streamed{Value: &pb.Streamed_Tag{Tag: x}}
	default:
		panic(fmt.Errorf("WrapStreamed: unsupported type: %T", v))
	}
}

func WrapArray[M proto.Message](msgs []M) ([]*pb.Stored, int) {
	size := int(0)
	stored := make([]*pb.Stored, len(msgs))
	idx := 0
	for _, m := range msgs {
		stored[idx] = WrapStored(m)
		size += proto.Size(stored[idx])
		idx += 1
	}
	return stored, size
}
