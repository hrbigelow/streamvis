#include "postgres.h"
#include "enc_typ_cache.h"
#include "enc_typ_core.h"
#include "sv_utils.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"
#include "pg_khash.h"

PG_MODULE_MAGIC;

typedef struct {
  int window_size;
  int stride;
  float4 *ring;
  int ring_head;
  int ring_count;
  uint64 global_idx;
} RingState;

// KHASH_INIT(grouphash)

typedef struct {
  // grouphash state;
  TupleDesc enc_typ_tupdesc;
} WindowState;


static float4
ring_mean(RingState *r) {
  float8 sum = 0.0;
  for (int i = 0; i < r->window_size; i++)
    sum += r->ring[(r->ring_head + i) % r->window_size];
  return (float4)(sum / r->window_size);
}

static void
ring_push(RingState *r, float4 v) {
  if (r->ring_count < r->window_size) {
    r->ring[(r->ring_head + r->ring_count) % r->window_size] = v;
    r->ring_count++;
  } else {
    r->ring[r->ring_head] = v;
    r->ring_head = (r->ring_head + 1) % r->window_size;
  }
}

PG_FUNCTION_INFO_V1(window_avg_sfunc);

Datum
window_avg_sfunc(PG_FUNCTION_ARGS) {
  WindowState *ctx;
  MemoryContext aggcontext;
  Datum *groups, d_data_type;
  Oid data_typoid;
  bool *nulls, isnull;
  int num_groups, out_size;

  int window_size = PG_GETARG_INT32(1);
  int stride = PG_GETARG_INT32(2);
  HeapTupleHeader vals = PG_GETARG_HEAPTUPLEHEADER(3);
  ArrayType *group_vals = PG_GETARG_ARRAYTYPE_P(4);

  HeapTupleData vals_enc = wrap_header(vals); 
  TupleDesc enc_desc = acquire_tupdesc(vals);

  // fdt_cache_init();

  if (!AggCheckCallContext(fcinfo, &aggcontext)) {
    elog(ERROR, "window_avg_sfunc called in non-aggregate context");
  }

  if (PG_ARGISNULL(0)) {
    Oid enc_typoid;
    TupleDesc tmp_tupdesc;
    MemoryContext oldctx;

    ctx = (WindowState *) MemoryContextAllocZero(aggcontext, sizeof(WindowState));
    enc_typoid = get_fn_expr_argtype(fcinfo->flinfo, 3);
    if (!OidIsValid(enc_typoid))
      elog(ERROR, "Couldn't determine type of argument 3");

    tmp_tupdesc = lookup_rowtype_tupdesc(enc_typoid, -1);
    oldctx = MemoryContextSwitchTo(aggcontext);
    ctx->enc_typ_tupdesc = CreateTupleDescCopy(tmp_tupdesc);
    MemoryContextSwitchTo(oldctx);
    ReleaseTupleDesc(tmp_tupdesc);
  } else {
    ctx = (WindowState *) DatumGetPointer(PG_GETARG_DATUM(0));
  }

  if (PG_ARGISNULL(3)) {
    PG_RETURN_POINTER(DatumGetPointer(PG_GETARG_DATUM(0)));
  }

  deconstruct_array(
      group_vals, enc_desc->tdtypeid, -1, false, 'i', &groups, &nulls, &num_groups);

  d_data_type = heap_getattr(&vals_enc, 1, enc_desc, &isnull);

  if (!isnull) {
    Oid t = DatumGetObjectId(d_data_type);
    if (t == fdt_cache.label_oid[FDT_INT]) {
        int *ints = enc_typ_to_ints(&vals_enc, enc_desc, &out_size);
    }
    else if (t == fdt_cache.label_oid[FDT_FLOAT]) {
    }
    else if (t == fdt_cache.label_oid[FDT_TEXT]) {
    }
    else if (t == fdt_cache.label_oid[FDT_BOOL]) {
    }
    else {
      elog(ERROR, "unrecognized field_data_typ oid %u", t);
    }
  }
  ReleaseTupleDesc(enc_desc);
  PG_RETURN_POINTER(ctx);
}



/*
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
*/



