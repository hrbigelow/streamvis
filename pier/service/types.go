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
	Shape      []uint32         `db:"shape"`
	IntBase    *[]int32         `db:"int_base"`
	FloatBase  *[]float32       `db:"float_base"`
	BoolBase   *[]bool          `db:"bool_base"`
	StringBase *[]string        `db:"text_base"`
	IntSpans   *[]pgtype.Int4   `db:"int_spans"`
	FloatSpans *[]pgtype.Float4 `db:"float_spans"`
	Bcast      *[]bool          `db:"bcast"`
}

func NewEncTypValue(msg *pb.EncTyp) (*EncTypValue, error) {
	v := &EncTypValue{
		Shape: msg.Shape,
	}
	switch b := msg.Base.Value.(type) {
	case *pb.AnyArray_Ints:
		v.IntBase = &b.Ints.Values
	case *pb.AnyArray_Floats:
		v.FloatBase = &b.Floats.Values
	case *pb.AnyArray_Strings:
		v.StringBase = &b.Strings.Values
	case *pb.AnyArray_Bools:
		v.BoolBase = &b.Bools.Values
	}

	if msg.GetIntSpans() != nil {
		vals := msg.GetIntSpans().GetValues()
		spans := make([]pgtype.Int4, len(vals))
		v.IntSpans = &spans
		for i, opt := range vals {
			if opt.Value != nil {
				spans[i] = pgtype.Int4{Int32: *opt.Value, Valid: true}
			}
		}
	}

	if msg.GetFloatSpans() != nil {
		vals := msg.GetFloatSpans().GetValues()
		spans := make([]pgtype.Float4, len(vals))
		v.FloatSpans = &spans
		for i, opt := range vals {
			if opt.Value != nil {
				spans[i] = pgtype.Float4{Float32: *opt.Value, Valid: true}
			}
		}
	}

	if msg.GetBcast() != nil {
		bcast := msg.GetBcast().GetValues()
		v.Bcast = &bcast
	}

	return v, nil
}

func (ev *EncTypValue) toProtobuf() pb.EncTyp {
	msg := pb.EncTyp{
		Shape: ev.Shape,
		Base:  &pb.AnyArray{},
	}
	if ev.IntBase != nil {
		msg.Base.Value = &pb.AnyArray_Ints{Ints: &pb.IntArray{Values: *ev.IntBase}}
	}
	if ev.FloatBase != nil {
		msg.Base.Value = &pb.AnyArray_Floats{Floats: &pb.FloatArray{Values: *ev.FloatBase}}
	}
	if ev.BoolBase != nil {
		msg.Base.Value = &pb.AnyArray_Bools{Bools: &pb.BoolArray{Values: *ev.BoolBase}}
	}
	if ev.StringBase != nil {
		msg.Base.Value = &pb.AnyArray_Strings{Strings: &pb.StringArray{Values: *ev.StringBase}}
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
			// else {
			// 	fvals[i] = &pb.OptionalFloat{Value: nil}
			// }
		}
		msg.Spans = &pb.EncTyp_FloatSpans{FloatSpans: &pb.FloatValues{Values: fvals}}
	}
	if ev.Bcast != nil {
		msg.Spans = &pb.EncTyp_Bcast{Bcast: &pb.BoolArray{Values: *ev.Bcast}}
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

func NewAttributeFilterValue(pb *pb.AttributeFilter) (AttributeFilterValue, error) {
	fieldHandle, err := uuid.Parse(pb.GetFieldHandle())
	if err != nil {
		return AttributeFilterValue{}, err
	}

	v := AttributeFilterValue{
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

type FieldValue struct {
	Handle    uuid.UUID `db:"field_handle"`
	IntVal    *int32    `db:"int_val"`
	FloatVal  *float32  `db:"float_val"`
	BoolVal   *bool     `db:"bool_val"`
	StringVal *string   `db:"string_val"`
}

type FullFieldValue struct {
	Handle    uuid.UUID `db:"handle"`
	Name      string    `db:"name"`
	IntVal    *int32    `db:"int_val"`
	FloatVal  *float32  `db:"float_val"`
	BoolVal   *bool     `db:"bool_val"`
	StringVal *string   `db:"string_val"`
}

func (fv FullFieldValue) toProtobuf() (pb.FieldValue, error) {
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

func NewFieldValue(msg *pb.FieldValue) (FieldValue, error) {
	handle, err := uuid.Parse(msg.GetHandle())
	if err != nil {
		return FieldValue{}, err
	}
	ret := FieldValue{
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

type Field struct {
	Handle      uuid.UUID `db:"handle"`
	Name        string    `db:"name"`
	DataType    string    `db:"data_type"`
	Description string    `db:"description"`
}

func (ft Field) toProtobuf() (pb.Field, error) {
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

type Coord struct {
	CoordHandle uuid.UUID `db:"coord_handle"`
	FieldHandle uuid.UUID `db:"field_handle"`
	Name        string    `db:"name"`
	DataType    string    `db:"data_type"`
	Description string    `db:"description"`
}

func (co Coord) toProtobuf() (pb.Coord, error) {
	dataType, err := dataTypeToProtobuf(co.DataType)
	if err != nil {
		return pb.Coord{}, err
	}

	msg := pb.Coord{
		CoordHandle: co.CoordHandle.String(),
		FieldHandle: co.FieldHandle.String(),
		Name:        co.Name,
		DataType:    dataType,
		Description: co.Description,
	}
	return msg, nil
}

type Series struct {
	Handle uuid.UUID `db:"handle"`
	Name   string    `db:"name"`
	Coords []*Coord  `db:"coords"`
}

func (sr Series) toProtobuf() (pb.Series, error) {
	msg := pb.Series{
		Name:   sr.Name,
		Handle: sr.Handle.String(),
	}
	msg.Coords = make([]*pb.Coord, len(sr.Coords))
	for i, coord := range sr.Coords {
		pbcoord, err := coord.toProtobuf()
		if err != nil {
			return msg, err
		}
		msg.Coords[i] = &pbcoord
	}
	return msg, nil
}

type Run struct {
	Handle    uuid.UUID         `db:"handle"`
	Tags      []string          `db:"tags"`
	StartedAt time.Time         `db:"started_at"`
	Attrs     []*FullFieldValue `db:"attrs"`
	Series    []*Series         `db:"series"`
}

func (rr Run) toProtobuf() (pb.Run, error) {
	msg := pb.Run{
		Handle:    rr.Handle.String(),
		Tags:      rr.Tags,
		StartedAt: timestamppb.New(rr.StartedAt),
	}
	msg.Attrs = make(map[string]*pb.FieldValue)
	for _, attr := range rr.Attrs {
		pbvalue, err := attr.toProtobuf()
		if err != nil {
			return pb.Run{}, err
		}
		msg.Attrs[attr.Name] = &pbvalue
	}
	msg.Series = make(map[string]*pb.Series)
	for _, series := range rr.Series {
		pbvalue, err := series.toProtobuf()
		if err != nil {
			return pb.Run{}, err
		}
		msg.Series[series.Name] = &pbvalue
	}

	return msg, nil
}

type RunId struct {
	Handle uuid.UUID `db:"handle"`
}

func (r RunId) toProtobuf() (pb.RunId, error) {
	msg := pb.RunId{
		Handle: r.Handle.String(),
	}
	return msg, nil
}

type RunStartTime struct {
	StartedAt time.Time `db:"started_at"`
}

func (rst RunStartTime) toProtobuf() (pb.RunStartTime, error) {
	msg := pb.RunStartTime{
		StartedAt: timestamppb.New(rst.StartedAt),
	}
	return msg, nil
}

type TagValue struct {
	Tag string `db:"tag"`
}

func (tv TagValue) toProtobuf() (pb.TagValue, error) {
	msg := pb.TagValue{
		Tag: tv.Tag,
	}
	return msg, nil
}

type TagFilterValue struct {
	PosTags     []string `db:"pos_tags"`
	PosMatchAll bool     `db:"pos_match_all"`
	NegTags     []string `db:"neg_tags"`
	NegMatchAll bool     `db:"neg_match_all"`
}

func NewTagFilterValue(msg *pb.TagFilter) (TagFilterValue, error) {
	if msg == nil {
		return TagFilterValue{}, fmt.Errorf("Received nil tag_filter")
	}
	pos_tags := make([]string, len(msg.PosTags))
	for i, tag := range msg.PosTags {
		pos_tags[i] = tag
	}
	neg_tags := make([]string, len(msg.NegTags))
	for i, tag := range msg.NegTags {
		neg_tags[i] = tag
	}
	val := TagFilterValue{
		PosTags:     pos_tags,
		PosMatchAll: msg.PosMatchAll,
		NegTags:     neg_tags,
		NegMatchAll: msg.NegMatchAll,
	}
	return val, nil
}

type RunFilter struct {
	AttributeFilters []AttributeFilterValue
	TagFilter        TagFilterValue
	MinStartedAt     *time.Time
	MaxStartedAt     *time.Time
}

func NewRunFilter(msg *pb.RunFilter) (RunFilter, error) {
	if msg == nil {
		return RunFilter{}, fmt.Errorf("Received nil pb.RunFilter")
	}
	rf := RunFilter{}
	rf.AttributeFilters = make([]AttributeFilterValue, len(msg.AttributeFilters))
	var err error
	for i, filter := range msg.GetAttributeFilters() {
		rf.AttributeFilters[i], err = NewAttributeFilterValue(filter)
		if err != nil {
			return RunFilter{}, err
		}
	}
	rf.TagFilter, err = NewTagFilterValue(msg.GetTagFilter())
	if err != nil {
		return RunFilter{}, err
	}
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
	RunHandle uuid.UUID      `db:"run_handle"`
	EncVals   []*EncTypValue `db:"enc_vals"`
}

func (cd ChunkData) toProtobuf() (pb.ChunkData, error) {
	encVals := make([]*pb.EncTyp, len(cd.EncVals))
	for i, encVal := range cd.EncVals {
		val := encVal.toProtobuf()
		encVals[i] = &val
	}

	msg := pb.ChunkData{
		EncVals: encVals,
	}
	return msg, nil
}

type AttributeValues struct {
	Field   Field      `db:"field"`
	Ints    *[]int32   `db:"ints"`
	Floats  *[]float32 `db:"floats"`
	Bools   *[]bool    `db:"bools"`
	Strings *[]string  `db:"strings"`
}

func (av AttributeValues) toProtobuf() (pb.AttributeValues, error) {
	field, err := av.Field.toProtobuf()
	if err != nil {
		return pb.AttributeValues{}, err
	}
	msg := pb.AttributeValues{
		Field:  &field,
		Values: &pb.AnyArray{},
	}
	if av.Ints != nil {
		msg.Values.Value = &pb.AnyArray_Ints{Ints: &pb.IntArray{Values: *av.Ints}}
	}
	if av.Floats != nil {
		msg.Values.Value = &pb.AnyArray_Floats{Floats: &pb.FloatArray{Values: *av.Floats}}
	}
	if av.Bools != nil {
		msg.Values.Value = &pb.AnyArray_Bools{Bools: &pb.BoolArray{Values: *av.Bools}}
	}
	if av.Strings != nil {
		msg.Values.Value = &pb.AnyArray_Strings{Strings: &pb.StringArray{Values: *av.Strings}}
	}
	return msg, err
}
