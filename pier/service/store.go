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

	arrayType, err := conn.LoadType(ctx, "enc_typ[]")
	if err != nil {
		return err
	}
	conn.TypeMap().RegisterType(arrayType)
	return nil
}

func unwrapAttributesMap(attrs map[string]*pb.Attribute) map[string]any {
	result := make(map[string]any, len(attrs))
	for key, attr := range attrs {
		if attr == nil {
			continue
		}
		switch v := attr.Value.(type) {
		case *pb.Attribute_IntVal:
			result[key] = v.IntVal
		case *pb.Attribute_FloatVal:
			result[key] = v.FloatVal
		case *pb.Attribute_TextVal:
			result[key] = v.TextVal
		case *pb.Attribute_BoolVal:
			result[key] = v.BoolVal
		}
	}
	return result
}

func (st *Store) CreateAttribute(
	ctx context.Context,
	attrName string,
	attrType string,
	attrDesc string,
) error {
	sql := `CALL create_attribute($1, $2, $3)`
	_, err := st.pool.Exec(ctx, sql, attrName, attrType, attrDesc)
	return err
}

func (st *Store) CreateSeries(
	ctx context.Context,
	seriesName string,
	seriesStructure map[string]string,
) error {
	sql := `CALL create_series($1, $2)`
	_, err := st.pool.Exec(ctx, sql, seriesName, seriesStructure)
	return err
}

func (st *Store) AppendToSeries(
	ctx context.Context,
	seriesHandle uuid.UUID,
	runHandle uuid.UUID,
	fieldName []string,
	fieldVals []*pb.EncTyp,
) (bool, error) {
	wrapped := make([]*EncTypValue, len(fieldVals))
	for i, et := range fieldVals {
		wrapped[i] = NewEncTypValue(et)
	}

	var success bool
	sql := `CALL append_to_series($1, $2, $3, $4, $5)`

	err := st.pool.QueryRow(
		ctx, sql, seriesHandle, runHandle, fieldName, wrapped, nil,
	).Scan(&success)

	if err != nil {
		return false, fmt.Errorf("failed to call append_to_series: %w\n", err)
	}
	return success, nil
}

func (st *Store) CreateRun(
	ctx context.Context,
) (uuid.UUID, error) {
	sql := `CALL create_run($1)`
	var runHandle uuid.UUID
	err := st.pool.QueryRow(ctx, sql, nil).Scan(&runHandle)
	if err != nil {
		return uuid.Nil, fmt.Errorf("error calling delete_run: %w\n", err)
	}
	return runHandle, nil
}

func (st *Store) DeleteRun(
	ctx context.Context,
	handle uuid.UUID,
) (bool, error) {
	var success bool
	sql := `CALL delete_run($1, $2)`

	err := st.pool.QueryRow(
		ctx, sql, handle, nil,
	).Scan(&success)

	if err != nil {
		return false, fmt.Errorf("error calling delete_run: %w\n", err)
	}
	return success, nil
}

func (st *Store) SetRunAttributes(
	ctx context.Context,
	handle uuid.UUID,
	attrs map[string]*pb.Attribute,
) error {
	sql := `CALL set_run_attributes($1, $2)`
	unwrapped := unwrapAttributesMap(attrs)
	_, err := st.pool.Exec(ctx, sql, handle, unwrapped)
	return err
}

func queryItems[T any](
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	args ...any,
) (<-chan *T, <-chan error) {
	dataCh := make(chan *T, 10)
	errCh := make(chan error, 1)

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		errCh <- err
		return dataCh, errCh
	}

	go func() {
		defer close(dataCh)
		defer rows.Close()

		for rows.Next() {
			item, err := pgx.RowToStructByName[T](rows)
			if err != nil {
				errCh <- err
				return
			}
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case dataCh <- &item:
			}
		}
	}()
	return dataCh, errCh
}

func (st *Store) ListSeries(
	ctx context.Context,
) (<-chan *pb.ListSeriesResponse, <-chan error) {
	sql := `SELECT * from series_vw`
	return queryItems[pb.ListSeriesResponse](ctx, st.pool, sql)
}

func (st *Store) DeleteEmptySeries(
	ctx context.Context,
	seriesName string,
) error {
	sql := `CALL delete_empty_series($1)`
	_, err := st.pool.Exec(ctx, sql, seriesName)
	return err
}

func (st *Store) ListAttributes(
	ctx context.Context,
) (<-chan *pb.ListAttributesResponse, <-chan error) {
	sql := `SELECT * from attribute_vw`
	return queryItems[pb.ListAttributesResponse](ctx, st.pool, sql)
}
