#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"

PG_MODULE_MAGIC;

typedef struct {
  int window_size;
  int stride;
  float4 *ring;
  int ring_head;
  int ring_count;
  uint64 global_idx;
} WindowState;

static float4
ring_mean(WindowState *s) {
  float8 sum = 0.0;
  for (int i = 0; i < s->window_size; i++)
    sum += s->ring[(s->ring_head + i) % s->window_size];
  return (float4)(sum / s->window_size);
}

static void
ring_push(WindowState *s, float4 v) {
  if (s->ring_count < s->window_size) {
    s->ring[(s->ring_head + s->ring_count) % s->window_size] = v;
    s->ring_count++;
  } else {
    s->ring[s->ring_head] = v;
    s->ring_head = (s->ring_head + 1) % s->window_size;
  }
}



PG_FUNCTION_INFO_V1(window_avg);

Datum
window_avg(PG_FUNCTION_ARGS) {
  int window_size = PG_GETARG_INT32(0);
  int stride = PG_GETARG_INT32(1);
  ArrayType *vals_arr = PG_GETARG_ARRAYTYPE_P(2);

  WindowState *s = (WindowState *) fcinfo->flinfo->fn_extra;
  if (s == NULL) {
    MemoryContext oldctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
    s = palloc0(sizeof(WindowState));
    s->window_size = window_size;
    s->stride = stride;
    s->ring = palloc0(sizeof(float4) * window_size);

    MemoryContextSwitchTo(oldctx);
    fcinfo->flinfo->fn_extra = s;
  }

  int nvals;
  int out_len;
  float4 *vals, *out;

  if (ARR_NDIM(vals_arr) != 1)
    ereport(ERROR,
        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
         errmsg("window_avg: vals must be 1-D array")));

  nvals = ARR_DIMS(vals_arr)[0];
  vals = (float4 *) ARR_DATA_PTR(vals_arr);

  out = palloc(sizeof(float4) * (nvals / s->stride) + 1); // TODO: check this!
  out_len = 0;

  for (int i = 0; i < nvals; i++) {
    ring_push(s, vals[i]);
    if (s->ring_count == s->window_size &&
        s->global_idx % (uint64) s->stride == 0) {
      out[out_len++] = ring_mean(s);
    }
    s->global_idx++;
  }

  int data_size = out_len * sizeof(float4);
  int total_size = ARR_OVERHEAD_NONULLS(1) + data_size;
  ArrayType *arr = (ArrayType *) palloc0(total_size);

  SET_VARSIZE(arr, total_size);
  ARR_NDIM(arr) = 1;
  ARR_ELEMTYPE(arr) = FLOAT4OID;
  ARR_DIMS(arr)[0] = out_len;
  ARR_LBOUND(arr)[0] = 1;

  if (out_len > 0)
    memcpy(ARR_DATA_PTR(arr), out, data_size);

  PG_RETURN_ARRAYTYPE_P(arr);

}


