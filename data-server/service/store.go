package service

import (
	"context"
	"regexp"

	pb "data-server/pb/data"
)

type Store interface {
	GetData(
		scopePat, namePat *regexp.Regexp,
		minOffset uint64,
		ctx context.Context,
	) (<-chan *pb.Data, <-chan error)

	GetMaxId() uint32

	GetScopes(scopePat *regexp.Regexp) []string

	GetNames(scopePat, namePat *regexp.Regexp) [][2]string

	GetConfigs(
		scopePat *regexp.Regexp,
		minOffset uint64,
		ctx context.Context,
	) (<-chan *pb.Config, <-chan error)
}
