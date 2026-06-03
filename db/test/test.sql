SET work_mem = '512MB';

SELECT ARRAY(
	SELECT run_id from list_runs_internal(
		'{}'::attribute_filter_typ[], 
		ROW('{100k-eqns}', TRUE, '{}', FALSE)::tag_filter_typ, NULL, NULL
	)
) AS p_run_ids
\gset

SELECT ARRAY[
	'fa7101fd-1d55-4af8-a9bf-f75c84878393', 
	'a5d6e43e-4f64-4ea7-9f7f-6dd9e51dc95f', 
	'd7934b16-f2c1-4489-9148-aa2f3d3bbf40'
]::UUID[] AS p_coord_handles
\gset

SELECT 'fa7101fd-1d55-4af8-a9bf-f75c84878393'::UUID AS p_order_coord_handle
\gset

SELECT ARRAY[
  'e2375246-7b19-426d-bf30-161e0263f249' -- data_split
]::UUID[] AS p_group_coord_handles
\gset

SELECT 10 AS p_window_size, 100 AS p_stride
\gset

SELECT 0 AS p_begin_chunk_id, 10000000 AS p_end_chunk_id
\gset

SELECT DISTINCT series_id AS p_series_id FROM coord WHERE handle = ANY(:'p_coord_handles'::UUID[])
\gset


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT *
FROM 
	get_coord_data_windowed_impl(
		:'p_run_ids'::INT[],
		:'p_series_id'::INT,
		:'p_coord_handles'::UUID[],
		:'p_group_coord_handles'::UUID[],
		:'p_order_coord_handle'::UUID,
		:'p_window_size'::INT,
		:'p_stride'::INT,
		:'p_begin_chunk_id'::BIGINT,
		:'p_end_chunk_id'::BIGINT
	);

