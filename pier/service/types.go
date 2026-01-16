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
	Base     []byte          `db:"base"`
	Shape    []uint32        `db:"shape"`
	I32Spans []pgtype.Int4   `db:"int_spans"`
	F32Spans []pgtype.Float4 `db:"float_spans"`
}

func NewEncTypValue(pb *pb.EncTyp) *EncTypValue {
	v := &EncTypValue{
		Base:  pb.Base,
		Shape: pb.Shape,
	}

	if pb.GetIval() != nil {
		vals := pb.GetIval().GetValues()
		v.I32Spans = make([]pgtype.Int4, len(vals))
		for i, opt := range vals {
			if opt.Value != nil {
				v.I32Spans[i] = pgtype.Int4{Int32: *opt.Value, Valid: true}
			}
		}
	}

	if pb.GetFval() != nil {
		vals := pb.GetFval().GetValues()
		v.F32Spans = make([]pgtype.Float4, len(vals))
		for i, opt := range vals {
			if opt.Value != nil {
				v.F32Spans[i] = pgtype.Float4{Float32: *opt.Value, Valid: true}
			}
		}
	}

	return v
}

type AttributeFilterValue struct {
	AttrHandle     uuid.UUID `db:"attr_handle"`
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
	attrHandle, err := uuid.Parse(pb.GetAttrHandle())
	if err != nil {
		return nil, err
	}

	v := &AttributeFilterValue{
		AttrHandle:     attrHandle,
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
	Handle    uuid.UUID `db:"handle"`
	DataType  string    `db:"data_type"`
	IntVal    int32     `db:"int_val"`
	FloatVal  float32   `db:"float_val"`
	BoolVal   bool      `db:"bool_val"`
	StringVal string    `db:"string_val"`
}

func (fv FieldValueTyp) toProtobuf() (pb.FieldValue, error) {
	dataType, err := dataTypeToProtobuf(fv.DataType)
	if err != nil {
		return pb.FieldValue{}, err
	}
	msg := pb.FieldValue{
		Handle:   fv.Handle.String(),
		DataType: dataType,
	}
	switch dataType {
	case pb.FieldDataType_FIELD_DATA_TYPE_INT:
		msg.Value = &pb.FieldValue_IntVal{IntVal: fv.IntVal}
	case pb.FieldDataType_FIELD_DATA_TYPE_FLOAT:
		msg.Value = &pb.FieldValue_FloatVal{FloatVal: fv.FloatVal}
	case pb.FieldDataType_FIELD_DATA_TYPE_STRING:
		msg.Value = &pb.FieldValue_StringVal{StringVal: fv.StringVal}
	case pb.FieldDataType_FIELD_DATA_TYPE_BOOL:
		msg.Value = &pb.FieldValue_BoolVal{BoolVal: fv.BoolVal}
	}
	return msg, nil
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

func NewFieldValueTyp(msg *pb.FieldValue) (*FieldValueTyp, error) {
	handle, err := uuid.Parse(msg.GetHandle())
	if err != nil {
		return nil, err
	}
	ret := &FieldValueTyp{
		Handle: handle,
	}

	switch msg.GetDataType() {
	case pb.FieldDataType_FIELD_DATA_TYPE_INT:
		ret.IntVal = msg.GetIntVal()
		ret.DataType = "int"
	case pb.FieldDataType_FIELD_DATA_TYPE_FLOAT:
		ret.FloatVal = msg.GetFloatVal()
		ret.DataType = "float"
	case pb.FieldDataType_FIELD_DATA_TYPE_STRING:
		ret.StringVal = msg.GetStringVal()
		ret.DataType = "string"
	case pb.FieldDataType_FIELD_DATA_TYPE_BOOL:
		ret.BoolVal = msg.GetBoolVal()
		ret.DataType = "bool"
	}
	return ret, nil
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

type SeriesResponse struct {
	SeriesName   string      `db:"name"`
	SeriesHandle uuid.UUID   `db:"handle"`
	Fields       []*FieldTyp `db:"fields"`
}

func (sr SeriesResponse) toProtobuf() (pb.ListSeriesResponse, error) {
	msg := pb.ListSeriesResponse{
		SeriesName:   sr.SeriesName,
		SeriesHandle: sr.SeriesHandle.String(),
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

type RunsResponse struct {
	RunHandle uuid.UUID        `db:"handle"`
	Tags      []string         `db:"tags"`
	StartedAt time.Time        `db:"started_at"`
	Attrs     []*FieldValueTyp `db:"attrs"`
}

func (rr RunsResponse) toProtobuf() (pb.ListRunsResponse, error) {
	msg := pb.ListRunsResponse{
		RunHandle: rr.RunHandle.String(),
		Tags:      rr.Tags,
		StartedAt: timestamppb.New(rr.StartedAt),
	}
	msg.Attrs = make([]*pb.FieldValue, len(rr.Attrs))
	for i, attr := range rr.Attrs {
		pbvalue, err := attr.toProtobuf()
		if err != nil {
			return pb.ListRunsResponse{}, err
		}
		msg.Attrs[i] = &pbvalue
	}

	return msg, nil
}

type TagFilterValue struct {
	HasAnyTag  []string `db:"has_any_tag"`
	HasAllTags []string `db:"has_all_tags"`
}
