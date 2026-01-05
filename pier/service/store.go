package service

import (
	"context"
	"fmt"
	"time"

	pb "pier/pb/streamvis/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, dbUri string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dbUri)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnIdleTime = 30 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour

	config.AfterConnect = registerCustomTypes
	pool, err2 := pgxpool.NewWithConfig(ctx, config)
	if err2 != nil {
		return nil, err2
	}
	return &Store{pool: pool}, nil
}

func registerCustomTypes(
	ctx context.Context,
	conn *pgx.Conn,
) error {
	dbType, err := conn.LoadType(ctx, "enc_typ")
	if err != nil {
		return err
	}
	conn.TypeMap().RegisterType(dbType)
	return nil
}

func (st *Store) MakeOrGetScope(
	ctx context.Context,
	scopeName string,
	deleteExisting bool,
) (uuid.UUID, error) {
	var scopeHandle uuid.UUID
	sql := `CALL make_or_get_scope($1, $2, $3)`
	err := st.pool.QueryRow(ctx, sql, scopeName, deleteExisting, nil).Scan(&scopeHandle)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to call make_or_get_scope: %w\n", err)
	}
	return scopeHandle, nil
}

func (st *Store) DeleteScope(
	ctx context.Context,
	scopeHandle uuid.UUID,
) (bool, error) {
	var deleted bool
	sql := `CALL delete_scope($1, $2)`
	err := st.pool.QueryRow(ctx, sql, scopeHandle, nil).Scan(&deleted)
	if err != nil {
		return false, fmt.Errorf("failed to call delete_scope: %w\n", err)
	}
	return deleted, nil
}

func (st *Store) MakeOrGetSeries(
	ctx context.Context,
	scopeHandle uuid.UUID,
	seriesName string,
	seriesStructure map[string]string,
	deleteExisting bool,
) (uuid.UUID, error) {
	var seriesHandle uuid.UUID
	sql := `CALL make_or_get_series($1, $2, $3, $4, $5)`
	err := st.pool.QueryRow(
		ctx, sql, scopeHandle, seriesName, seriesStructure, deleteExisting, nil,
	).Scan(&seriesHandle)

	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to call make_or_get_series: %w\n", err)
	}
	return seriesHandle, nil
}

type EncTyp struct {
	Base     []byte    `db:"base"`
	Shape    []int32   `db:"shape"`
	I32Spans []int32   `db:"i32_spans"`
	F32Spans []float32 `db:"f32_spans"`
}

func (st *Store) AppendToSeries(
	ctx context.Context,
	seriesHandle uuid.UUID,
	fieldName []string,
	fieldVals []*pb.EncTyp,
) (bool, error) {
	wrapped := make([]*EncTypValue, len(fieldVals))
	for i, et := range fieldVals {
		wrapped[i] = &EncTypValue{et}
	}

	var success bool
	sql := `CALL append_to_series($1, $2, $3, $4)`

	err := st.pool.QueryRow(
		ctx, sql, seriesHandle, fieldName, wrapped, nil,
	).Scan(&success)

	if err != nil {
		return false, fmt.Errorf("failed to call append_to_series: %w\n", err)
	}
	return success, nil
}
