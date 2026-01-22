package service

import (
	"fmt"
	pb "pier/pb/streamvis/v1"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jackc/pgx/v5/pgtype"
)

type ToProtobuffer[B any] interface {
	toProtobuf() (B, error)
}

func MakeToProtobufFunc[A ToProtobuffer[B], B any]() func(A) (B, error) {
	return func(a A) (B, error) {
		return a.toProtobuf()
	}
}

type EncTypValue struct {
	FieldHandle uuid.UUID        `db:"field_handle"`
	Base        []byte           `db:"base"`
	Shape       []uint32         `db:"shape"`
	IntSpans    *[]pgtype.Int4   `db:"int_spans"`
	FloatSpans  *[]pgtype.Float4 `db:"float_spans"`
	BoolBcast   *[]bool          `db:"bool_bcast"`
	StringBcast *[]bool          `db:string_bcast"`
}

func NewEncTypValue(pb *pb.EncTyp) (*EncTypValue, error) {
	fieldHandle, err := uuid.Parse(pb.GetFieldHandle())
	if err != nil {
		return nil, err
	}
	v := &EncTypValue{
		FieldHandle: fieldHandle,
		Base:        pb.Base,
		Shape:       pb.Shape,
	}

	if pb.GetIntSpans() != nil {
		vals := pb.GetIntSpans().GetValues()
		spans := make([]pgtype.Int4, len(vals))
		v.IntSpans = &spans
		for i, opt := range vals {
			if opt.Value != nil {
				spans[i] = pgtype.Int4{Int32: *opt.Value, Valid: true}
			}
		}
	}

	if pb.GetFloatSpans() != nil {
		vals := pb.GetFloatSpans().GetValues()
		spans := make([]pgtype.Float4, len(vals))
		v.FloatSpans = &spans
		for i, opt := range vals {
			if opt.Value != nil {
				spans[i] = pgtype.Float4{Float32: *opt.Value, Valid: true}
			}
		}
	}

	if pb.GetBoolBcast() != nil {
		bcast := pb.GetBoolBcast().GetValues()
		v.BoolBcast = &bcast
	}

	if pb.GetStringBcast() != nil {
		bcast := pb.GetStringBcast().GetValues()
		v.StringBcast = &bcast
	}

	return v, nil
}

func (ev *EncTypValue) toProtobuf() pb.EncTyp {
	msg := pb.EncTyp{
		FieldHandle: ev.FieldHandle.String(),
		Base:        ev.Base,
		Shape:       ev.Shape,
	}
	if ev.IntSpans != nil {
		ivals := make([]*pb.OptionalInt, len(*ev.IntSpans))
		for i, i4 := range *ev.IntSpans {
			if i4.Valid {
				ivals[i] = &pb.OptionalInt{Value: &i4.Int32}
			}
		}
		msg.Spans = &pb.EncTyp_IntSpans{IntSpans: &pb.IntValues{Values: ivals}}
	}
	if ev.FloatSpans != nil {
		fvals := make([]*pb.OptionalFloat, len(*ev.FloatSpans))
		for i, f4 := range *ev.FloatSpans {
			if f4.Valid {
				fvals[i] = &pb.OptionalFloat{Value: &f4.Float32}
			}
		}
		msg.Spans = &pb.EncTyp_FloatSpans{FloatSpans: &pb.FloatValues{Values: fvals}}
	}
	if ev.BoolBcast != nil {
		msg.Spans = &pb.EncTyp_BoolBcast{BoolBcast: &pb.BoolValues{Values: *ev.BoolBcast}}
	}
	if ev.StringBcast != nil {
		msg.Spans = &pb.EncTyp_StringBcast{StringBcast: &pb.BoolValues{Values: *ev.StringBcast}}
	}
	return msg
}

type AttributeFilterValue struct {
	FieldHandle    uuid.UUID `db:"field_handle"`
	IncludeMissing bool      `db:"include_missing"`
	IntMin         int32     `db:"int_min"`
	IntMax         int32     `db:"int_max"`
	IntVals        []int32   `db:"int_vals"`
	FloatMin       float32   `db:"float_min"`
	FloatMax       float32   `db:"float_max"`
	BoolVals       []bool    `db:"bool_vals"`
	StringVals     []string  `db:"string_vals"`
}

func NewAttributeFilterValue(pb *pb.AttributeFilter) (*AttributeFilterValue, error) {
	fieldHandle, err := uuid.Parse(pb.GetFieldHandle())
	if err != nil {
		return nil, err
	}

	v := &AttributeFilterValue{
		FieldHandle:    fieldHandle,
		IncludeMissing: pb.GetIncludeMissing(),
	}

	if val := pb.GetIntRange(); val != nil {
		v.IntMin = val.GetImin()
		v.IntMax = val.GetImax()
	}
	if val := pb.GetIntList(); val != nil {
		v.IntVals = val.Vals
	}
	if val := pb.GetFloatRange(); val != nil {
		v.FloatMin = val.GetFmin()
		v.FloatMax = val.GetFmax()
	}
	if val := pb.GetBoolList(); val != nil {
		v.BoolVals = val.Vals
	}
	if val := pb.GetStringList(); val != nil {
		v.StringVals = val.Vals
	}
	return v, nil
}

type FieldValueTyp struct {
	Handle    uuid.UUID `db:"field_handle"`
	IntVal    *int32    `db:"int_val"`
	FloatVal  *float32  `db:"float_val"`
	BoolVal   *bool     `db:"bool_val"`
	StringVal *string   `db:"string_val"`
}

func (fv FieldValueTyp) toProtobuf() (pb.FieldValue, error) {
	msg := pb.FieldValue{
		Handle: fv.Handle.String(),
	}
	valuesSet := 0
	if fv.IntVal != nil {
		valuesSet++
	}
	if fv.FloatVal != nil {
		valuesSet++
	}
	if fv.BoolVal != nil {
		valuesSet++
	}
	if fv.StringVal != nil {
		valuesSet++
	}

	if valuesSet != 1 {
		return msg, fmt.Errorf("Exactly one value must be set.  Got %d", valuesSet)
	}

	if fv.IntVal != nil {
		msg.Value = &pb.FieldValue_IntVal{IntVal: *fv.IntVal}
	}
	if fv.FloatVal != nil {
		msg.Value = &pb.FieldValue_FloatVal{FloatVal: *fv.FloatVal}
	}
	if fv.BoolVal != nil {
		msg.Value = &pb.FieldValue_BoolVal{BoolVal: *fv.BoolVal}
	}
	if fv.StringVal != nil {
		msg.Value = &pb.FieldValue_StringVal{StringVal: *fv.StringVal}
	}
	return msg, nil
}

func NewFieldValueTyp(msg *pb.FieldValue) (FieldValueTyp, error) {
	handle, err := uuid.Parse(msg.GetHandle())
	if err != nil {
		return FieldValueTyp{}, err
	}
	ret := FieldValueTyp{
		Handle: handle,
	}
	switch v := msg.Value.(type) {
	case *pb.FieldValue_IntVal:
		ret.IntVal = &v.IntVal
	case *pb.FieldValue_FloatVal:
		ret.FloatVal = &v.FloatVal
	case *pb.FieldValue_BoolVal:
		ret.BoolVal = &v.BoolVal
	case *pb.FieldValue_StringVal:
		ret.StringVal = &v.StringVal
	}
	return ret, nil
}

func dataTypeToProtobuf(data_type string) (pb.FieldDataType, error) {
	switch data_type {
	case "int":
		return pb.FieldDataType_FIELD_DATA_TYPE_INT, nil
	case "float":
		return pb.FieldDataType_FIELD_DATA_TYPE_FLOAT, nil
	case "string":
		return pb.FieldDataType_FIELD_DATA_TYPE_STRING, nil
	case "bool":
		return pb.FieldDataType_FIELD_DATA_TYPE_BOOL, nil
	default:
		dt := pb.FieldDataType_FIELD_DATA_TYPE_UNSPECIFIED
		err := fmt.Errorf(
			"received data type %s.  Must be one of (int, float, string, bool)", data_type)
		return dt, err
	}
}

type FieldTyp struct {
	Handle      uuid.UUID `db:"handle"`
	Name        string    `db:"name"`
	DataType    string    `db:"data_type"`
	Description string    `db:"description"`
}

func (ft FieldTyp) toProtobuf() (pb.Field, error) {
	dataType, err := dataTypeToProtobuf(ft.DataType)
	if err != nil {
		return pb.Field{}, err
	}

	msg := pb.Field{
		Handle:      ft.Handle.String(),
		Name:        ft.Name,
		DataType:    dataType,
		Description: ft.Description,
	}
	return msg, nil
}

type Series struct {
	Name   string      `db:"name"`
	Handle uuid.UUID   `db:"handle"`
	Fields []*FieldTyp `db:"fields"`
}

func (sr Series) toProtobuf() (pb.Series, error) {
	msg := pb.Series{
		Name:   sr.Name,
		Handle: sr.Handle.String(),
	}
	msg.Fields = make([]*pb.Field, len(sr.Fields))
	for i, field := range sr.Fields {
		pbfield, err := field.toProtobuf()
		if err != nil {
			return msg, err
		}
		msg.Fields[i] = &pbfield
	}
	return msg, nil
}

type Run struct {
	Handle    uuid.UUID        `db:"handle"`
	Tags      []string         `db:"tags"`
	StartedAt time.Time        `db:"started_at"`
	Attrs     []*FieldValueTyp `db:"attrs"`
}

func (rr Run) toProtobuf() (pb.Run, error) {
	msg := pb.Run{
		Handle:    rr.Handle.String(),
		Tags:      rr.Tags,
		StartedAt: timestamppb.New(rr.StartedAt),
	}
	msg.Attrs = make([]*pb.FieldValue, len(rr.Attrs))
	for i, attr := range rr.Attrs {
		pbvalue, err := attr.toProtobuf()
		if err != nil {
			return pb.Run{}, err
		}
		msg.Attrs[i] = &pbvalue
	}

	return msg, nil
}

type RunId struct {
	Handle uuid.UUID `db:handle"`
}

func (r RunId) toProtobuf() (pb.RunId, error) {
	msg := pb.RunId{
		Handle: r.Handle.String(),
	}
	return msg, nil
}

type TagFilterValue struct {
	HasAnyTag  *[]string `db:"has_any_tag"`
	HasAllTags *[]string `db:"has_all_tags"`
}

func NewTagFilterValue(msg *pb.TagFilter) TagFilterValue {
	var hasAny, hasAll *[]string
	if msg.GetHasAnyTag() != nil {
		hasAny = &msg.GetHasAnyTag().Vals
	}
	if msg.GetHasAllTags() != nil {
		hasAll = &msg.GetHasAllTags().Vals
	}

	val := TagFilterValue{
		HasAnyTag:  hasAny,
		HasAllTags: hasAll,
	}
	return val
}

// TODO: update DB to mirror this struct
type RunFilter struct {
	AttributeFilters []*AttributeFilterValue
	TagFilter        TagFilterValue
	MinStartedAt     *time.Time
	MaxStartedAt     *time.Time
}

func NewRunFilter(msg *pb.RunFilter) (RunFilter, error) {
	rf := RunFilter{}
	rf.AttributeFilters = make([]*AttributeFilterValue, len(msg.AttributeFilters))
	var err error
	for i, filter := range msg.GetAttributeFilters() {
		rf.AttributeFilters[i], err = NewAttributeFilterValue(filter)
		if err != nil {
			return RunFilter{}, err
		}
	}
	rf.TagFilter = NewTagFilterValue(msg.GetTagFilter())
	if msg.MinStartedAt != nil {
		t := msg.MinStartedAt.AsTime()
		rf.MinStartedAt = &t
	}
	if msg.MaxStartedAt != nil {
		t := msg.MaxStartedAt.AsTime()
		rf.MaxStartedAt = &t
	}
	return rf, nil
}

type ChunkData struct {
	ChunkId int64          `db:"chunk_id"`
	EncVals []*EncTypValue `db:"enc_val"`
}

func (cd ChunkData) toProtobuf() (pb.ChunkData, error) {
	encVals := make([]*pb.EncTyp, len(cd.EncVals))
	for i, encVal := range cd.EncVals {
		val := encVal.toProtobuf()
		encVals[i] = &val
	}

	msg := pb.ChunkData{
		ChunkId: cd.ChunkId,
		EncVals: encVals,
	}
	return msg, nil
}
