package service

import (
	pb "pier/pb/streamvis/v1"
)

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
			return v.GetIval().GetValues()
		}
		return nil
	case 3:
		if v.GetFval() != nil {
			return v.GetFval().GetValues()
		}
		return nil
	}
	return nil
}

func (v *EncTypValue) IsNull() bool {
	return v.EncTyp == nil
}

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
