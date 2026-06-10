#include "postgres.h"
#include "fmgr.h"

#include "sv_utils.h"

/*
CREATE TYPE enc_typ AS (
	shape INT[],
	int_base INT[],
	float_base REAL[],
	bool_base BOOLEAN[],
	text_base TEXT[],
	int_spans INT[],
	float_spans REAL[],
	bcast BOOLEAN[]
);
*/

typedef struct {
  int *shape;
  int n_dim;
  int *int_base;
  float *float_base;
  bool *bool_base;
  const char **text_base;
  int *int_spans;
  float *float_spans;
  bool *bcast;
  bool *nulls;
} EncTyp;

static EncTyp
parse_enc_typ(HeapTuple enc, TupleDesc tupdesc) {
  Datum values[8];
  bool nulls[8], *dummy;
  int sz;
  EncTyp e;
  ArrayType *ary;

  heap_deform_tuple(enc, tupdesc, values, nulls);

  if (!nulls[0])
    e.shape = array_to_ints(DatumGetArrayTypeP(values[0]), &dummy, &e.n_dim);
  if (!nulls[1])
    e.int_base = array_to_ints(DatumGetArrayTypeP(values[1]), &dummy, &sz);
  if (!nulls[2])
    e.float_base = array_to_floats(DatumGetArrayTypeP(values[2]), &dummy, &sz);
  if (!nulls[3])
    e.bool_base = array_to_bools(DatumGetArrayTypeP(values[3]), &dummy, &sz);
  if (!nulls[4])
    e.text_base = array_to_texts(DatumGetArrayTypeP(values[4]), &dummy, &sz);
  if (!nulls[5]) {
    ary = DatumGetArrayTypeP(values[5]);
    e.int_spans = array_to_ints(ary, &e.nulls, &sz);
  }
  if (!nulls[6]) {
    ary = DatumGetArrayTypeP(values[6]);
    e.float_spans = array_to_floats(ary, &e.nulls, &sz);
  }
  if (!nulls[7]) {
    ary = DatumGetArrayTypeP(values[7]);
    e.bcast = array_to_bools(ary, &e.nulls, &sz);
  }
  return e;
}

static void
ravel_coord(int idx, const int *dims, int ndims, int *coords) {
  // given `idx` in [0, prod(dims)), compute the coords
  for (int d = ndims-1; d != -1; d--) {
    coords[d] = idx % dims[d];
    idx /= dims[d];
  }
  return;
}

static int
flatten_coord(int *coords, const int *dims, int ndims) {
  // return idx corresponding to the flattened coords
  int idx = 0;
  int stride = 1;
  for (int d = ndims-1; d != -1; d--) {
    idx += coords[d] * stride;
    stride *= dims[d];
  }
  return idx;
}

static int
product(int *dims, int ndims) {
  int p = 1;
  for (int i = 0; i != ndims; i++) {
    p *= dims[i];
  }
  return p;
}

static void
mask_coord(int *in_coord, bool *no_span, int *out_coord, int ndim) {
  for (int i = 0; i != ndim; i++) {
    out_coord[i] = no_span[i] ? in_coord[i] : 0;
  }
}


PG_FUNCTION_INFO_V1(decode_int_enc_v1);

Datum
decode_int_enc_v1(PG_FUNCTION_ARGS) {

  HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
  HeapTupleData enc = wrap_header(rec);
  TupleDesc enc_desc = acquire_tupdesc(rec);
  EncTyp e = parse_enc_typ(&enc, enc_desc);
  int nelem = product(e.shape, e.n_dim);
  int *result = (int *) palloc(nelem * sizeof(int));
  int dst_coord[10]; // excess
  int src_coord[10], src_shape[10];
  int src_idx, incr;
  for (int d = 0; d != e.n_dim; d++) {
    src_shape[d] = e.nulls[d] ? e.shape[d] : 1;
  }

  for (int idx = 0; idx != nelem; idx++) {
    ravel_coord(idx, e.shape, e.n_dim, dst_coord);
    mask_coord(dst_coord, e.nulls, src_coord, e.n_dim);
    src_idx = flatten_coord(src_coord, src_shape, e.n_dim);
    incr = 0;
    for (int d = 0; d != e.n_dim; d++) {
      if (! e.nulls[d])
        incr += dst_coord[d] * (e.int_spans[d] / e.shape[d]);
    }
    result[idx] = e.int_base[src_idx] + incr;
  }
  ReleaseTupleDesc(enc_desc);
  PG_RETURN_ARRAYTYPE_P(ints_to_array(result, nelem));
}


PG_FUNCTION_INFO_V1(decode_float_enc_v1);

Datum
decode_float_enc_v1(PG_FUNCTION_ARGS) {

  HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
  HeapTupleData enc = wrap_header(rec);
  TupleDesc enc_desc = acquire_tupdesc(rec);
  EncTyp e = parse_enc_typ(&enc, enc_desc);
  int nelem = product(e.shape, e.n_dim);
  float *result = (float *) palloc(nelem * sizeof(float));
  int dst_coord[10]; // excess
  int src_coord[10], src_shape[10];
  int src_idx;
  float incr;
  for (int d = 0; d != e.n_dim; d++) {
    src_shape[d] = e.nulls[d] ? e.shape[d] : 1;
  }

  for (int idx = 0; idx != nelem; idx++) {
    ravel_coord(idx, e.shape, e.n_dim, dst_coord);
    mask_coord(dst_coord, e.nulls, src_coord, e.n_dim);
    src_idx = flatten_coord(src_coord, src_shape, e.n_dim);
    incr = 0.0;
    for (int d = 0; d != e.n_dim; d++) {
      if (! e.nulls[d])
        incr += (float)dst_coord[d] * (e.float_spans[d] / (float)e.shape[d]);
    }
    result[idx] = e.float_base[src_idx] + incr;
  }
  ReleaseTupleDesc(enc_desc);
  PG_RETURN_ARRAYTYPE_P(floats_to_array(result, nelem));
}


PG_FUNCTION_INFO_V1(decode_text_enc_v1);

Datum
decode_text_enc_v1(PG_FUNCTION_ARGS) {

  HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
  HeapTupleData enc = wrap_header(rec);
  TupleDesc enc_desc = acquire_tupdesc(rec);
  EncTyp e = parse_enc_typ(&enc, enc_desc);
  int nelem = product(e.shape, e.n_dim);
  const char **result = (const char **) palloc(nelem * sizeof(char*));
  int dst_coord[10]; // excess
  int src_coord[10], src_shape[10];
  bool no_span[10];
  int src_idx;
  for (int d = 0; d != e.n_dim; d++) {
    src_shape[d] = e.bcast[d] ? 1 : e.shape[d];
    no_span[d] = ! e.bcast[d];
  }

  for (int idx = 0; idx != nelem; idx++) {
    ravel_coord(idx, e.shape, e.n_dim, dst_coord);
    mask_coord(dst_coord, no_span, src_coord, e.n_dim);
    src_idx = flatten_coord(src_coord, src_shape, e.n_dim);
    result[idx] = e.text_base[src_idx];
  }
  ReleaseTupleDesc(enc_desc);
  PG_RETURN_ARRAYTYPE_P(texts_to_array(result, nelem));
}

