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

EncTyp
parse_enc_typ(HeapTuple enc, TupleDesc tupdesc) {
  Datum values[8];
  bool nulls[8];
  int sz;
  EncTyp e;
  ArrayType *ary;

  heap_deform_tuple(enc, tupdesc, values, nulls);

  if (!nulls[0])
    e.shape = array_to_ints(DatumGetArrayTypeP(values[0]), &e.n_dim);
  if (!nulls[1])
    e.int_base = array_to_ints(DatumGetArrayTypeP(values[1]), &sz);
  if (!nulls[2])
    e.float_base = array_to_floats(DatumGetArrayTypeP(values[2]), &sz);
  if (!nulls[3])
    e.bool_base = array_to_bools(DatumGetArrayTypeP(values[3]), &sz);
  if (!nulls[4])
    e.text_base = array_to_texts(DatumGetArrayTypeP(values[4]), &sz);
  if (!nulls[5]) {
    ary = DatumGetArrayTypeP(values[5]);
    deconstruct_array(ary, INT4OID, 4, false, 'i', &span, &e.nulls, &sz);

  }
  if (!nulls[6]) {
    ary = DatumGetArrayTypeP(values[6]);
    deconstruct_array(ary, FLOAT4OID, 4, false, 'i', &e.float_spans, &e.nulls, &sz);
  }
  if (!nulls[7]) {
    ary = DatumGetArrayTypeP(values[7]);
    deconstruct_array(ary, BOOLOID, 1, false, 'i', &e.bcast, &e.nulls, &sz);
  }
  return e;
}

void ravel_coord(int idx, const int *dims, int ndims, int *coords) {
  // given `idx` in [0, prod(dims)), compute the coords
  for (int d = ndims-1; d != -1; d--) {
    coords[d] = idx % dims[d];
    idx /= dims[d];
  }
  return;
}

int flatten_coord(int *coords, const int *dims, int ndims) {
  // return idx corresponding to the flattened coords
  int idx = 0;
  int stride = 1;
  for (int d = ndims-1; d != -1; d--) {
    idx += coords[d] * stride;
    stride *= dims[d];
  }
  return idx;
}

int product(int *dims, int ndims) {
  int p = 1;
  for (int i = 0; i != ndims; i++) {
    p *= dims[i];
  }
  return p;
}

void mask_coord(int *in_coord, bool *nulls, int *out_coord, int ndim) {
  for (int i = 0; i != ndim; i++) {
    out_coord[i] = nulls[i] ? 0 : in_coord[i];
  }
}


PG_FUNCTION_INFO_V1(decode_int_enc_v1);

Datum
decode_int_enc_v1(PG_FUNCTION_ARGS) {

  HeapTupleHeader rec = PG_GETARG_HEAPTUPLEHEADER(0);
  HeapTupleData enc = wrap_header(rec);
  TupleDesc enc_desc = acquire_tupdesc(rec);
  EncTyp e = parse_enc_typ(&enc, enc_desc);
  int nelem = product(e.shape, e.n_dims);
  int *result = (int *) palloc(nelem * sizeof(int));
  int dst_coord[10]; // excess
  int src_coord[10];

  for (int idx = 0; idx != nelem; idx++) {
    ravel_coord(idx, e.shape, e.n_dims, dst_coord);
    mask_coord(dst_coord, e.nulls, src_coord, e.n_dims);
    src_idx = flatten_coord(src_coord, e.shape, e.n_dims);
    incr = 0;
    for (int d = 0; d != e.n_dim; d++) {
      if (! e.nulls[d])
        incr += dst_coord[d] * (e.int_spans[d] / e.shape[d]);
    }
    result[idx] = e.int_base[src_idx] + incr;
  }
  PG_RETURN_ARRAYTYPE_P(ints_to_array(result, nelem));
}


