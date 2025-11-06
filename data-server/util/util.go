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

/*
A carry value when merging data.  This will exist in one of two states.

 1. data is non-nil and NextStart is 0.
    This state represents that the next window will consume `data`

 2. data is nil and NextStart > 0
    This means that the next window doesn't occur and more data needs to be processed.
*/
type DataWithOffset struct {
	Data      *pb.Data
	NextStart uint32
}

func dataByteLength(data *pb.Data) uint32 {
	return uint32(len(data.Axes[0].Data))
}

/*
Given input `data`, computes a slice of that data starting at `offset`
Returns:
*pb.Data - the new slice (copying underlying memory)
uint32 - the new offset
*/
func sliceData(data *pb.Data, offset uint32) (*pb.Data, uint32) {
	result := &pb.Data{
		EntryId: data.EntryId,
		Index:   data.Index,
		NameId:  data.NameId,
		Axes:    make([]*pb.Axis, len(data.Axes)),
	}
	bytes := dataByteLength(data)
	newOffset := uint32(0)
	if bytes < offset {
		newOffset += offset - bytes
		offset = bytes
	}

	for i := range data.Axes {
		source := data.Axes[i].Data[offset:]
		dest := make([]byte, len(source))
		copy(dest, source)

		result.Axes[i] = &pb.Axis{
			Dtype:  data.Axes[i].Dtype,
			Length: uint32(len(dest)),
			Data:   dest,
		}
	}
	return result, newOffset
}

/*
 */
func MergeData(carry DataWithOffset, data *pb.Data) (DataWithOffset, error) {

	if carry.NextStart > 0 {
		newData, newOffset := sliceData(data, carry.NextStart)
		return DataWithOffset{
			Data:      newData,
			NextStart: newOffset,
		}, nil
	}
	if carry.Data.NameId != carry.Data.NameId || data.Index != data.Index {
		return DataWithOffset{}, fmt.Errorf(
			"MergeData: cannot merge data from different (name_id, index) combinations")
	}

	merged := &pb.Data{
		EntryId: data.EntryId,
		Index:   data.Index,
		NameId:  data.NameId,
		Axes:    make([]*pb.Axis, len(data.Axes)),
	}

	for i := range carry.Data.Axes {
		if carry.Data.Axes[i].Dtype != data.Axes[i].Dtype {
			return DataWithOffset{}, fmt.Errorf(
				"MergeData: pb.Data from same (name_id, index) pairs have different Axis dtype")
		}
		combinedData := make([]byte, 0, dataByteLength(carry.Data)+dataByteLength(data))
		combinedData = append(combinedData, carry.Data.Axes[i].Data...)
		combinedData = append(combinedData, data.Axes[i].Data...)

		merged.Axes[i] = &pb.Axis{
			Dtype:  carry.Data.Axes[i].Dtype,
			Length: uint32(len(combinedData)),
			Data:   combinedData,
		}
	}

	return DataWithOffset{
		Data:      merged,
		NextStart: 0,
	}, nil
}

/*
Apply windowFn to windows of data, interpreting it as pb.DType and collecting the result.
Returns:

		result - the computed window values for windows every `stride` steps
	    offset - next window position
		error -
*/
func windowFilter(
	data []byte,
	dtype pb.DType,
	stride int,
	windowSize int,
	windowFn func(data []float64) float64,
) ([]byte, uint32, error) {
	n := len(data)
	if n < windowSize {
		return []byte{}, 0, nil
	}
	numVals := n / 4
	numWinPositions := numVals - windowSize + 1              // total number of window positions possible
	numStridedWin := (numWinPositions + stride - 1) / stride // # complete windows in [0, n) when striding
	nextWinPos := uint32(numStridedWin * stride * 4)

	vals := make([]float64, numVals)
	results := make([]byte, numStridedWin*4)

	if (numStridedWin-1)*stride+windowSize > len(vals) {
		panic("incorrectly sized vals")
	}

	// extract vals as float64
	for i := 0; i < numVals; i++ {
		u := binary.LittleEndian.Uint32(data[i*4 : (i+1)*4])
		if dtype == pb.DType_D_TYPE_I32 {
			vals[i] = float64(int32(u))
		} else if dtype == pb.DType_D_TYPE_F32 {
			vals[i] = float64(math.Float32frombits(u))
		} else {
			return make([]byte, 0), 0, fmt.Errorf("Invalid DType")
		}
	}

	// apply windowFn over vals
	for w := 0; w < numStridedWin; w++ {
		fval := windowFn(vals[w*stride : (w*stride)+windowSize])
		dest := results[w*4 : (w+1)*4]
		if dtype == pb.DType_D_TYPE_I32 {
			uval := uint32(int32(math.Round(fval)))
			binary.LittleEndian.PutUint32(dest, uval)
		} else if dtype == pb.DType_D_TYPE_F32 {
			uval := math.Float32bits(float32(fval))
			binary.LittleEndian.PutUint32(dest, uval)
		} else {
			return make([]byte, 0), 0, fmt.Errorf("Invalid DType")
		}
	}
	return results, nextWinPos, nil

}

/*
Applies the sampling strategy to data
Returns:

		*pb.Data - the result of the sampling
	    dataWithOffset - information for continuing
		error    - any error occurring
*/
func ApplySamplingToData(
	carry DataWithOffset,
	stride int,
	windowSize int,
	windowFn func(data []float64) float64,
) (*pb.Data, DataWithOffset, error) {
	data := carry.Data
	if data.Axes[0].Length < uint32(windowSize) {
		return nil, carry, nil
	}

	result := &pb.Data{
		EntryId: data.EntryId,
		Index:   data.Index,
		NameId:  data.NameId,
		Axes:    make([]*pb.Axis, len(data.Axes)),
	}

	// the relative position in new data where the next window occurs.
	var offset uint32

	for a := range data.Axes {
		axis := data.Axes[a]
		win, nextBytePos, err := windowFilter(axis.Data, axis.Dtype, stride, windowSize, windowFn)
		if err != nil {
			return result, DataWithOffset{}, fmt.Errorf("Error applying window Filter for axis %d", a)
		}

		result.Axes[a] = &pb.Axis{
			Dtype:  axis.Dtype,
			Length: uint32(len(win)),
			Data:   win,
		}
		offset = nextBytePos
	}

	remain, _ := sliceData(data, offset)

	newCarry := DataWithOffset{
		Data:      remain,
		NextStart: offset,
	}

	return result, newCarry, nil
}

func MeanReduction(window []float64) float64 {
	sum := float64(0)
	for i := 0; i != len(window); i++ {
		sum += window[i]
	}
	return sum / float64(len(window))
}

var Reductions = map[pb.Reduction]func([]float64) float64{
	pb.Reduction_REDUCTION_MEAN: MeanReduction,
}
