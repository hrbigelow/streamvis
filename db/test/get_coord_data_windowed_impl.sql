LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_analyze = TRUE;
SET auto_explain.log_buffers = TRUE;
SET auto_explain.log_format = 'text';

DO $$
DECLARE
	p_series_id INT;
	p_run_ids INT[];
	p_attribute_filters attribute_filter_typ[] := '{}';
	p_coord_handles UUID[] := ARRAY[
		'fa7101fd-1d55-4af8-a9bf-f75c84878393', -- sgd_step
		'a5d6e43e-4f64-4ea7-9f7f-6dd9e51dc95f', -- kldiv
		'd7934b16-f2c1-4489-9148-aa2f3d3bbf40' -- xent
	];
	p_group_coord_handles UUID[] := ARRAY[
	  'e2375246-7b19-426d-bf30-161e0263f249' -- data_split
	];
	p_order_coord_handle UUID := 'fa7101fd-1d55-4af8-a9bf-f75c84878393';
	p_window_size INT := 10;
	p_stride INT := 100;
	p_begin_chunk_id BIGINT := 0;
	p_end_chunk_id BIGINT := 1000000;
	p_tag_filter tag_filter_typ := ROW('{100k-eqns}', TRUE, '{}', FALSE);
BEGIN
	p_series_id := (
		SELECT DISTINCT series_id
		FROM coord
		WHERE handle = ANY(p_coord_handles)
	);

	p_run_ids := ARRAY(
		SELECT run_id FROM list_runs_internal(p_attribute_filters, p_tag_filter, NULL, NULL)
	);

	PERFORM set_config('auto_explain.log_nested_statements', 'on', FALSE);

	-- SELECT run_handle, enc_vals FROM 
	PERFORM 
	get_coord_data_windowed_impl(
		p_run_ids,
		p_series_id,
		p_coord_handles,
		p_group_coord_handles,
		p_order_coord_handle,
		p_window_size,
		p_stride,
		p_begin_chunk_id,
		p_end_chunk_id
	);

	PERFORM set_config('auto_explain.log_nested_statements', 'off', FALSE);

END;
$$;

