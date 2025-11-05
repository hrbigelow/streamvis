package util

import (
	"bufio"
	"bytes"
	pb "data-server/pb/streamvis/v1"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
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

func MergeData(d1, d2 *pb.Data) (*pb.Data, error) {
	if d1.NameId != d2.NameId || d1.Index != d2.Index {
		return nil, fmt.Errorf(
			"MergeData: cannot merge data from different (name_id, index) combinations")
	}
	if len(d1.Axes) != len(d1.Axes) {
		return nil, fmt.Errorf(
			"MergeData: pb.Data from same (name_id, index) pairs have different numbers of axes")
	}

	merged := &pb.Data{
		EntryId: d1.EntryId,
		Index:   d1.Index,
		NameId:  d1.NameId,
		Axes:    make([]*pb.Axis, len(d1.Axes)),
	}

	for i := range d1.Axes {
		if d1.Axes[i].Dtype != d2.Axes[i].Dtype {
			return nil, fmt.Errorf(
				"MergeData: pb.Data from same (name_id, index) pairs have different Axis dtype")
		}
		combinedData := make([]byte, 0, len(d1.Axes[i].Data)+len(d2.Axes[i].Data))
		combinedData = append(combinedData, d1.Axes[i].Data...)
		combinedData = append(combinedData, d2.Axes[i].Data...)

		merged.Axes[i] = &pb.Axis{
			Dtype:  d1.Axes[i].Dtype,
			Length: d1.Axes[i].Length + d2.Axes[i].Length,
			Data:   combinedData,
		}
	}
	return merged, nil
}

/*
Apply windowFn to windows of data, interpreting it as pb.DType and collecting the result.
Returns:

	result - the computed window values for windows every `stride` steps
	leftover - the bytes that would start the next window
	error -
*/
func windowFilter(
	data []byte,
	dtype pb.DType,
	stride int,
	windowSize int,
	windowFn func(data []float64) float64,
) ([]byte, []byte, error) {
	numVals := len(data) / 4
	numWin := (len(data) - windowSize + 1) / windowSize
	numRemain := (len(data) - windowSize + 1) % windowSize

	vals := make([]float64, numVals)
	results := make([]byte, numWin)
	remain := data[-numRemain:]

	for i := 0; i < numVals; i++ {
		u := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		if dtype == pb.DType_D_TYPE_I32 {
			vals[i] = float64(int32(u))
		} else if dtype == pb.DType_D_TYPE_F32 {
			vals[i] = float64(math.Float32frombits(u))
		} else {
			return make([]byte, 0), make([]byte, 0), fmt.Errorf("Invalid DType")
		}
	}

	for w := 0; w < numWin; w++ {
		fval := windowFn(vals[w*stride : (w*stride)+windowSize])
		dest := results[w*4 : (w+1)*4]
		if dtype == pb.DType_D_TYPE_I32 {
			uval := uint32(int32(fval))
			binary.LittleEndian.PutUint32(dest, uval)
		} else if dtype == pb.DType_D_TYPE_F32 {
			uval := uint32(float32(fval))
			binary.LittleEndian.PutUint32(dest, uval)
		} else {
			return make([]byte, 0), make([]byte, 0), fmt.Errorf("Invalid DType")
		}
	}
	return results, remain, nil

}

/*
 */
func ApplySamplingToData(
	data *pb.Data,
	stride int,
	windowSize int,
	windowFn func(data []float64) float64,
) (*pb.Data, *pb.Data, error) {

	result := &pb.Data{
		EntryId: data.EntryId,
		Index:   data.Index,
		NameId:  data.NameId,
		Axes:    make([]*pb.Axis, len(data.Axes)),
	}

	remain := &pb.Data{
		EntryId: data.EntryId,
		Index:   data.Index,
		NameId:  data.NameId,
		Axes:    make([]*pb.Axis, len(data.Axes)),
	}

	for a := range data.Axes {
		axis := data.Axes[a]
		win, extra, err := windowFilter(axis.Data, axis.Dtype, stride, windowSize, windowFn)
		if err != nil {
			return result, remain, fmt.Errorf("Error applying window Filter for axis %d", a)
		}

		result.Axes[a] = &pb.Axis{
			Dtype:  axis.Dtype,
			Length: uint32(len(win)),
			Data:   win,
		}

		remain.Axes[a] = &pb.Axis{
			Dtype:  axis.Dtype,
			Length: uint32(len(extra)),
			Data:   extra,
		}
	}

	return result, remain, nil
}
