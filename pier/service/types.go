package service

import (
	pb "pier/pb/streamvis/v1"

	"github.com/jackc/pgx/v5/pgtype"
)

type EncTypValue struct {
	Base     []byte          `db:"base"`
	Shape    []uint32        `db:"shape"`
	I32Spans []pgtype.Int4   `db:"i32_spans"`
	F32Spans []pgtype.Float4 `db:"f32_spans"`
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

/*
type EncTypValue struct {
	*pb.EncTyp
}

func (v *EncTypValue) Index(i int) any {
	switch i {
	case 0:
		return v.Base
	case 1:
		return v.Shape
	case 2:
		if v.GetIval() != nil {
			vals := v.GetIval().GetValues()
			result := make([]pgtype.Int4, len(vals))
			for i, opt := range vals {
				if opt.Value != nil {
					result[i] = pgtype.Int4{Int32: *opt.Value, Valid: true}
				}
			}
			return result
		}
		return nil
	case 3:
		if v.GetFval() != nil {
			vals := v.GetFval().GetValues()
			result := make([]pgtype.Float4, len(vals))
			for i, opt := range vals {
				if opt.Value != nil {
					result[i] = pgtype.Float4{Float32: *opt.Value, Valid: true}
				}
			}
			return result
		}
		return nil
	}
	return nil
}

func (v *EncTypValue) IsNull() bool {
	return v.EncTyp == nil
}
*/

/*
func (c *EncTypCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	// returns a struct implementing the EncodePlan interface (or nil)
	// the Encode member of EncodePlan converts the
	if _, ok := value.(pb.EncTyp); !ok {
		return nil
	}
	return pgtype.EncodePlanFunc(func(value any) (plan []byte, err error) {
		msg := value.(*pb.EncTyp)
		var builder pgtype.RecordBuilder
		builder.AppendValue(msg.Bytes)
		builder.AppendValue(msg.Shape)
		var ival *pb.IntValues
		var fval *pb.FloatValues
		if msg.Spans != nil {
			switch v := msg.Spans.(type) {
			case *pb.EncTyp_Ival:
				ival = v.Ival
			case *pb.EncTyp_Fval:
				fval = v.Fval
			}
		}
		builder.AppendValue(ival)
		builder.AppendValue(fval)
		return builder.Finish()
	})
}
*/
