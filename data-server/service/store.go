package service

import (
	"context"
	"regexp"

	pb "data-server/pb/data"

	"google.golang.org/protobuf/proto"
)

type Store interface {
	GetData(
		scopePat, namePat *regexp.Regexp,
		minOffset uint64,
		ctx context.Context,
	) (<-chan *pb.Data, <-chan error)

	GetConfigs(
		scopePat *regexp.Regexp,
		minOffset uint64,
		ctx context.Context,
	) (<-chan *pb.Config, <-chan error)

	GetRecordResult(scopePat, namePat *regexp.Regexp) pb.RecordResult

	GetMaxId() uint32

	GetScopes(scopePat *regexp.Regexp) []string

	GetNames(scopePat, namePat *regexp.Regexp) [][2]string

	Add(msg proto.Message)

	AddNames(names []*pb.Name) error

	AddDatas(datas []*pb.Data) error
}
