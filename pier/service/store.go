package service

import (
	"context"
	"fmt"
	"time"

	pb "pier/pb/streamvis/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	types := []string{
		"enc_typ",
		"enc_typ[]",
		"attribute_filter_typ",
		"attribute_filter_typ[]",
		"tag_filter_typ",
	}
	for _, ty := range types {
		dbType, err := conn.LoadType(ctx, ty)
		if err != nil {
			return err
		}
		conn.TypeMap().RegisterType(dbType)
	}
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

/*
func wrapAttributesMap(attrs map[string]any) map[string]*pb.Attribute {
	result := make(map[string]*pb.Attribute, len(attrs))
	for key, attr := range attrs {
		if attr == nil {
			continue
		}
		switch v := attr.Value.(type) {
		case float64:
			result[key] = &pb.Attribute{
		case *pb.Attribute_FloatVal:
			result[key] = v.FloatVal
		case *pb.Attribute_TextVal:
			result[key] = v.TextVal
		case *pb.Attribute_BoolVal:
			result[key] = v.BoolVal
		}
	}
	return result
*/

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

func (st *Store) ReplaceRun(
	ctx context.Context,
	runHandle uuid.UUID,
) error {
	sql := `CALL replace_run($1)`
	_, err := st.pool.Exec(ctx, sql, runHandle)
	return err
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

func queryItemsInternal[Row, Item any](
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	convert func(Row) (Item, error),
	args ...any,
) (<-chan *Item, <-chan error) {
	dataCh := make(chan *Item, 10)
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
			row, err := pgx.RowToStructByName[Row](rows)

			if err != nil {
				errCh <- err
				return
			}

			item, err := convert(row)

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

func queryItems[Item any](
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	args ...any,
) (<-chan *Item, <-chan error) {
	convert := func(row Item) (Item, error) { return row, nil }
	return queryItemsInternal(ctx, pool, sql, convert, args...)
}

func queryItemsConvert[Row, Item any](
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	convert func(Row) (Item, error),
	args ...any,
) (<-chan *Item, <-chan error) {
	return queryItemsInternal(ctx, pool, sql, convert, args...)
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

type RunResult struct {
	RunHandle string                    `db:"run_handle"`
	RunAttrs  map[string]map[string]any `db:"run_attrs"`
	StartedAt time.Time                 `db:"started_at"`
}

func (st *Store) ListRuns(
	ctx context.Context,
) (<-chan *pb.ListRunsResponse, <-chan error) {
	sql := `SELECT * from run_vw`
	convert := func(row RunResult) (pb.ListRunsResponse, error) {
		attrs := make(map[string]*pb.Attribute, len(row.RunAttrs))
		for k, v := range row.RunAttrs {
			if val, ok := v["int_val"]; ok {
				if intVal, ok2 := val.(float64); ok2 {
					attrs[k] = &pb.Attribute{Value: &pb.Attribute_IntVal{IntVal: int32(intVal)}}
				} else {
					err := fmt.Errorf("Attribute %s (int_val) was not a valid int32 value", row.RunHandle)
					return pb.ListRunsResponse{}, err
				}
			}
			if val, ok := v["float_val"]; ok {
				if floatVal, ok2 := val.(float64); ok2 {
					attrs[k] = &pb.Attribute{Value: &pb.Attribute_FloatVal{FloatVal: float32(floatVal)}}
				} else {
					err := fmt.Errorf("Attribute %s (float_val) was not a valid float32 value", row.RunHandle)
					return pb.ListRunsResponse{}, err
				}
			}
			if val, ok := v["text_val"]; ok {
				if textVal, ok2 := val.(string); ok2 {
					attrs[k] = &pb.Attribute{Value: &pb.Attribute_TextVal{TextVal: textVal}}
				} else {
					err := fmt.Errorf("Attribute %s (text_val) was not a valid string value", row.RunHandle)
					return pb.ListRunsResponse{}, err
				}
			}
			if val, ok := v["bool_val"]; ok {
				if boolVal, ok2 := val.(bool); ok2 {
					attrs[k] = &pb.Attribute{Value: &pb.Attribute_BoolVal{BoolVal: boolVal}}
				} else {
					err := fmt.Errorf("Attribute %s (bool_val) was not a valid bool value", row.RunHandle)
					return pb.ListRunsResponse{}, err
				}
			}
		}
		return pb.ListRunsResponse{
			RunHandle: row.RunHandle,
			RunAttrs:  attrs,
			StartedAt: timestamppb.New(row.StartedAt),
		}, nil
	}
	return queryItemsConvert(ctx, st.pool, sql, convert)
}
