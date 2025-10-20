package service

import (
	"context"
	"regexp"

	pb "data-server/pb/streamvis/v1"
)

type Store interface {
	// GetData finds all Data objects in the database whose scope and name match
	// scopePat and namePat, and whose BegOffset is >= minOffset.
	// The returned pb.RecordResult holds the owning Scopes and Names and the maximum
	// endOffset of the Data.  This can then be used as the minOffset for another
	// GetData request to progressively grab new data as it is written.
	GetData(
		scopePat, namePat *regexp.Regexp,
		minOffset uint64,
		ctx context.Context,
	) (pb.RecordResult, <-chan *pb.Data, <-chan error)

	// GetConfigs retrieves all Config objects in the database whose scope matches
	// scopePat, and streams them to the returned channel.  Also returns a
	// RecordResult holding the scopes.  Unlike GetData, GetConfigs does not filter
	// by offsets.  The returned RecordResult.FileOffset is always zero.
	GetConfigs(
		scopePat *regexp.Regexp,
		ctx context.Context,
	) (pb.RecordResult, <-chan *pb.Config, <-chan error)

	// GetMaxId retrieves the maximum Id used for any primary key in the store.
	GetMaxId() uint32

	// GetScopes retrieves the list of all scopes in the store matching scopePat
	GetScopes(scopePat *regexp.Regexp) []string

	// GetNames retrieves the list of all (scope, name) pairs in the store
	// matching scopePat and namePat
	GetNames(scopePat, namePat *regexp.Regexp) [][2]string

	// AddScope adds the pb.Scope to the store
	AddScope(scope *pb.Scope) error

	// AddConfig adds the pb.Config to the store
	AddConfig(config *pb.Config) error

	// AddNames adds the list of Name objects to the store
	AddNames(names []*pb.Name) error

	// AddDatas adds the list of Data objects to the store
	AddDatas(datas []*pb.Data) error

	// DeleteScopeNames logically deletes each (scope, name) pair from the single
	// provided scope and list of names
	DeleteScopeNames(scope string, names []string)
}
