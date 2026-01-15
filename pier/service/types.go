package service

import (
	pb "pier/pb/streamvis/v1"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgtype"
)

type RawJsonRow struct {
	JSONData []byte `db:"json_data"`
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

type TagFilterValue struct {
	HasAnyTag  []string `db:"has_any_tag"`
	HasAllTags []string `db:"has_all_tags"`
}
