package service

import (
	"context"
	"fmt"
	"time"

	pb "pier/pb/streamvis/v1"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const MAX_WIRE_SIZE = 4194304

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, dbUri string, doTrace bool) (*Store, error) {
	config, err := pgxpool.ParseConfig(dbUri)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnIdleTime = 30 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour

	config.AfterConnect = registerCustomTypes

	if doTrace {
		config.ConnConfig.Tracer = &tracelog.TraceLog{
			Logger: tracelog.LoggerFunc(
				func(
					ctx context.Context,
					level tracelog.LogLevel,
					msg string,
					data map[string]interface{},
				) {
					fmt.Printf("[%s] %s: %v\n", level, msg, data)
				}),
			LogLevel: tracelog.LogLevelDebug,
		}
		config.ConnConfig.OnNotice = func(pc *pgconn.PgConn, n *pgconn.Notice) {
			fmt.Printf("NOTICE: %s\n", n.Message)
		}
	}

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
		"field_data_typ",
		"field_typ",
		"field_typ[]",
		"coord_typ",
		"coord_typ[]",
		"enc_typ",
		"enc_typ[]",
		"series_typ",
		"series_typ[]",
		"field_value_typ",
		"field_value_typ[]",
		"full_field_value_typ",
		"full_field_value_typ[]",
		"attribute_filter_typ",
		"attribute_filter_typ[]",
		"tag_filter_typ",
		"tag_filter_typ[]",
	}
	for _, ty := range types {
		dbType, err := conn.LoadType(ctx, ty)
		if err != nil {
			return fmt.Errorf("failed to load type %s: %w", ty, err)
		}
		conn.TypeMap().RegisterType(dbType)
		// log.Printf("Registered type: %s (OID: %d)", ty, dbType.OID)
	}
	return nil

}

func (st *Store) CreateField(
	ctx context.Context,
	fieldName string,
	fieldType string,
	fieldDesc string,
) error {
	sql := `CALL create_field($1, $2, $3)`
	_, err := st.pool.Exec(ctx, sql, fieldName, fieldType, fieldDesc)
	return err
}

func (st *Store) CreateSeries(
	ctx context.Context,
	seriesName string,
	attrNames []string,
) error {
	sql := `CALL create_series($1, $2)`
	_, err := st.pool.Exec(ctx, sql, seriesName, attrNames)
	return err
}

func (st *Store) AppendToSeries(
	ctx context.Context,
	seriesHandle uuid.UUID,
	runHandle uuid.UUID,
	fieldVals []*pb.FullEncTyp,
) error {
	var err error
	encs := make([]*EncTypValue, len(fieldVals))
	handles := make([]uuid.UUID, len(fieldVals))

	for i, et := range fieldVals {
		handles[i], err = uuid.Parse(et.FieldHandle)
		if err != nil {
			return err
		}
		encs[i], err = NewEncTypValue(et.Enc)
		if err != nil {
			return err
		}
	}
	sql := `CALL append_to_series($1, $2, $3, $4)`
	_, err = st.pool.Exec(ctx, sql, seriesHandle, runHandle, handles, encs)
	return err
}

func (st *Store) CreateRun(ctx context.Context) (uuid.UUID, error) {
	sql := `CALL create_run($1)`
	var runHandle uuid.UUID
	err := st.pool.QueryRow(ctx, sql, nil).Scan(&runHandle)
	return runHandle, err
}

func (st *Store) ReplaceRun(ctx context.Context, runHandle uuid.UUID) error {
	sql := `CALL replace_run($1)`
	_, err := st.pool.Exec(ctx, sql, runHandle)
	return err
}

func (st *Store) DeleteRun(ctx context.Context, runHandle uuid.UUID) error {
	sql := `CALL delete_run($1)`
	_, err := st.pool.Exec(ctx, sql, runHandle)
	return err
}

func (st *Store) SetRunAttributes(
	ctx context.Context,
	handle uuid.UUID,
	attrs []*pb.FieldValue,
) error {
	sql := `CALL set_run_attributes($1, $2)`
	unwrapped := make([]FieldValue, len(attrs))
	var err error
	for i, attr := range attrs {
		unwrapped[i], err = NewFieldValue(attr)
		if err != nil {
			return err
		}
	}
	_, err = st.pool.Exec(ctx, sql, handle, unwrapped)
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
		close(errCh)
		close(dataCh)
		return dataCh, errCh
	}

	go func() {
		defer close(errCh)
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
		if err := rows.Err(); err != nil {
			errCh <- err
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
) (<-chan *pb.Series, <-chan error) {
	sql := `SELECT * from series_vw`
	convert := MakeToProtobufFunc[Series, pb.Series]()
	return queryItemsConvert(ctx, st.pool, sql, convert)
}

func (st *Store) DeleteEmptySeries(
	ctx context.Context,
	seriesName string,
) error {
	sql := `CALL delete_empty_series($1)`
	_, err := st.pool.Exec(ctx, sql, seriesName)
	return err
}

func (st *Store) AddRunTags(
	ctx context.Context,
	runHandle uuid.UUID,
	tags []string,
) error {
	sql := `CALL add_run_tags($1, $2)`
	_, err := st.pool.Exec(ctx, sql, runHandle, tags)
	return err
}

func (st *Store) DeleteRunTag(
	ctx context.Context,
	runHandle uuid.UUID,
	tag string,
) error {
	sql := `CALL delete_run_tag($1, $2)`
	_, err := st.pool.Exec(ctx, sql, runHandle, tag)
	return err
}

func (st *Store) ListFields(
	ctx context.Context,
) (<-chan *pb.Field, <-chan error) {
	sql := `SELECT * from field_vw`
	convert := MakeToProtobufFunc[Field, pb.Field]()
	return queryItemsConvert(ctx, st.pool, sql, convert)
}

func (st *Store) ListRuns(
	ctx context.Context,
	runFilter RunFilter,
) (<-chan *pb.Run, <-chan error) {
	sql := `SELECT * FROM list_runs($1, $2, $3, $4)`
	convert := MakeToProtobufFunc[Run, pb.Run]()
	return queryItemsConvert(
		ctx, st.pool, sql, convert,
		runFilter.AttributeFilters,
		runFilter.TagFilter,
		runFilter.MinStartedAt,
		runFilter.MaxStartedAt,
	)
}

func (st *Store) GetEndChunkId(
	ctx context.Context,
) (int64, error) {
	sql := `SELECT * FROM get_end_chunk_id()`
	var endId int64
	err := st.pool.QueryRow(ctx, sql).Scan(&endId)
	if err != nil {
		return 0, err
	}
	return endId, nil
}

func (st *Store) packRunChunks(
	ctx context.Context,
	dataCh <-chan *ChunkData,
	queryErrCh <-chan error,
) (<-chan *pb.RunChunks, <-chan error) {

	chunkCh := make(chan *pb.RunChunks)
	errCh := make(chan error, 1)

	rcmap := make(map[uuid.UUID]*pb.RunChunks)
	var rc *pb.RunChunks
	var ok bool
	sizes := make(map[uuid.UUID]int)
	const chunksField = 2
	var chunksTagSize = protowire.SizeTag(chunksField)

	go func() {
		defer close(chunkCh)
		defer close(errCh)

		for cd := range dataCh {
			cdmsg, err := cd.toProtobuf()
			if err != nil {
				errCh <- err
				return
			}
			payload := proto.Size(&cdmsg)
			addedSize := chunksTagSize + protowire.SizeBytes(payload)
			rc, ok = rcmap[cd.RunHandle]
			// add if not found
			if !ok {
				rc = &pb.RunChunks{
					RunHandle: cd.RunHandle.String(),
				}
				rcmap[cd.RunHandle] = rc
				sizes[cd.RunHandle] = proto.Size(rc)
			}
			// flush if next addition would be oversize
			if sizes[cd.RunHandle]+addedSize > MAX_WIRE_SIZE {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case chunkCh <- rc:
				}
				rc = &pb.RunChunks{
					RunHandle: cd.RunHandle.String(),
				}
				rcmap[cd.RunHandle] = rc
				sizes[cd.RunHandle] = proto.Size(rc)
			}
			// add
			rc.Chunks = append(rc.Chunks, &cdmsg)
			sizes[cd.RunHandle] += addedSize
		}
		for _, rc := range rcmap {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case chunkCh <- rc:
			}
		}
		for err := range queryErrCh {
			if err != nil {
				errCh <- err
				return
			}
		}
	}()
	return chunkCh, errCh
}

func (st *Store) QueryRunData(
	ctx context.Context,
	coordHandles []uuid.UUID,
	minChunkId *int64,
	maxChunkId *int64,
	runFilter RunFilter,
	windowSpec *WindowSpec,
) (<-chan *pb.RunChunks, <-chan error) {
	if windowSpec == nil {
		sql := `SELECT * from query_run_data($1, $2, $3, $4, $5, $6, $7)`
		dataCh, queryErrCh := queryItems[ChunkData](
			ctx, st.pool, sql,
			coordHandles,
			minChunkId,
			maxChunkId,
			runFilter.AttributeFilters,
			runFilter.TagFilter,
			runFilter.MinStartedAt,
			runFilter.MaxStartedAt,
		)
		return st.packRunChunks(ctx, dataCh, queryErrCh)
	}

	sql := `SELECT * from query_run_data_windowed($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	dataCh, queryErrCh := queryItems[ChunkData](
		ctx, st.pool, sql,
		coordHandles,
		windowSpec.GroupCoordHandles,
		windowSpec.OrderCoordHandle,
		windowSpec.Size,
		windowSpec.Stride,
		minChunkId,
		maxChunkId,
		runFilter.AttributeFilters,
		runFilter.TagFilter,
		runFilter.MinStartedAt,
		runFilter.MaxStartedAt,
	)
	return st.packRunChunks(ctx, dataCh, queryErrCh)
}

func (st *Store) ListCommonAttributes(
	ctx context.Context,
	runFilter RunFilter,
) (<-chan *pb.Field, <-chan error) {
	sql := `SELECT * from list_common_attributes($1, $2, $3, $4)`
	convert := MakeToProtobufFunc[Field, pb.Field]()
	return queryItemsConvert(
		ctx, st.pool, sql, convert,
		runFilter.AttributeFilters,
		runFilter.TagFilter,
		runFilter.MinStartedAt,
		runFilter.MaxStartedAt,
	)
}

func (st *Store) ListCommonSeries(
	ctx context.Context,
	runFilter RunFilter,
) (<-chan *pb.Series, <-chan error) {
	sql := `SELECT * from list_common_series($1, $2, $3, $4)`
	convert := MakeToProtobufFunc[Series, pb.Series]()
	return queryItemsConvert(
		ctx, st.pool, sql, convert,
		runFilter.AttributeFilters,
		runFilter.TagFilter,
		runFilter.MinStartedAt,
		runFilter.MaxStartedAt,
	)
}

func (st *Store) ListStartedAt(
	ctx context.Context,
) (<-chan *pb.RunStartTime, <-chan error) {
	sql := `SELECT * FROM started_at_vw`
	convert := MakeToProtobufFunc[RunStartTime, pb.RunStartTime]()
	return queryItemsConvert(ctx, st.pool, sql, convert)
}

func (st *Store) ListTags(
	ctx context.Context,
) (<-chan *pb.TagValue, <-chan error) {
	sql := `SELECT * FROM tag_vw`
	convert := MakeToProtobufFunc[TagValue, pb.TagValue]()
	return queryItemsConvert(ctx, st.pool, sql, convert)
}

func (st *Store) ListAttributeValues(
	ctx context.Context,
) (<-chan *pb.AttributeValues, <-chan error) {
	sql := `SELECT * FROM attribute_values_vw`
	convert := MakeToProtobufFunc[AttributeValues, pb.AttributeValues]()
	return queryItemsConvert(ctx, st.pool, sql, convert)
}
