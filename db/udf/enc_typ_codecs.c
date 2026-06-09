#include "postgres.h"
#include "enc_typ_codecs.h"
#include "enc_typ_cache.h"

#include "fmgr.h"
#include "funcapi.h"
#include "utils/array.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_enum.h"
#include "catalog/namespace.h"


/*
  Utilities for manipulating enc_typ, defined in db/schema/tables.sql

CREATE TYPE enc_typ AS (
  data_type field_data_typ,
  floats REAL[],
  bools BOOLEAN[],
  texts TEXT[],
  base INT,
  diff INT[],
  size INT
);
*/
typedef enum {
  ENC_TYP_FIELD_TYPE = 0,
  ENC_TYP_FLOATS,
  ENC_TYP_BOOLS,
  ENC_TYP_TEXTS,
  ENC_TYP_BASE,
  ENC_TYP_DIFF,
  ENC_TYP_SIZE,
  ENC_TYP_NATTS
} EncTypFields; 


FdtCache fdt_cache = { .valid = false };

static Oid
get_enum_label_oid(Oid enumtypoid, const char *label) {
  HeapTuple tup;
  Oid result;

  tup = SearchSysCache2(ENUMTYPOIDNAME, ObjectIdGetDatum(enumtypoid), CStringGetDatum(label));
  if (!HeapTupleIsValid(tup))
    elog(ERROR, "enum label \"%s\" not found for type %u", label, enumtypoid);

  result = ((Form_pg_enum) GETSTRUCT(tup))->oid;
  ReleaseSysCache(tup);
  return result;
}


void
fdt_cache_init(void) {
  Oid fdt_typ_oid;

  if (fdt_cache.valid)
    return;

  fdt_typ_oid = TypenameGetTypid("field_data_typ");
  if (!OidIsValid(fdt_typ_oid))
    elog(ERROR, "type field_data_typ not valid");

  fdt_cache.label_oid[0] = get_enum_label_oid(fdt_typ_oid, "int");
  fdt_cache.label_oid[1] = get_enum_label_oid(fdt_typ_oid, "float");
  fdt_cache.label_oid[2] = get_enum_label_oid(fdt_typ_oid, "text");
  fdt_cache.label_oid[3] = get_enum_label_oid(fdt_typ_oid, "bool");
  fdt_cache.valid = true;
}

TupleDesc
acquire_tupdesc(HeapTupleHeader rec) {
  Oid tupType;
  int32 tupTypmod;
  tupType = HeapTupleHeaderGetTypeId(rec);
  tupTypmod = HeapTupleHeaderGetTypMod(rec);
  return lookup_rowtype_tupdesc(tupType, tupTypmod);
}

HeapTupleData
wrap_header(HeapTupleHeader rec) {
  HeapTupleData tuple;
  tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
  ItemPointerSetInvalid(&(tuple.t_self));
  tuple.t_tableOid = InvalidOid;
  tuple.t_data = rec;
  return tuple;
}


float *
decode_float_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
  bool isnull;
  Datum d_floats;
  ArrayType *ary;

  d_floats = heap_getattr(enc, 2, tupdesc, &isnull);

  if (isnull) {
    return NULL;
  }

  ary = DatumGetArrayTypeP(d_floats);
  if (ARR_HASNULL(ary))
    elog(ERROR, "REAL[] contains null elements, raw pointer extraction unsafe.");

  *out_size = ArrayGetNItems(ARR_NDIM(ary), ARR_DIMS(ary));
  return (float *) ARR_DATA_PTR(ary);
}


static int *
expand_diff_array(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
  ArrayType *ary;
  int *diffs, *ints, diff_size;
  Datum values[ENC_TYP_NATTS];
  bool nulls[ENC_TYP_NATTS];

  heap_deform_tuple(enc, tupdesc, values, nulls);

  if (nulls[ENC_TYP_BASE] || nulls[ENC_TYP_DIFF] || nulls[ENC_TYP_SIZE]) {
    *out_size = 0;
    return NULL;
  }

  *out_size = DatumGetInt32(values[ENC_TYP_SIZE]);

  ary = DatumGetArrayTypeP(values[ENC_TYP_DIFF]);
  if (ARR_HASNULL(ary))
    elog(ERROR, "INT[] contains null elements, raw pointer extraction unsafe.");

  diffs = (int *)ARR_DATA_PTR(ary);

  diff_size = ArrayGetNItems(ARR_NDIM(ary), ARR_DIMS(ary));
  ints = (int *) palloc(*out_size * sizeof(int));
  if (*out_size == 0) {
    return ints;
  }
  ints[0] = DatumGetInt32(values[ENC_TYP_BASE]);
  for (int i=1; i != *out_size; i++) {
    ints[i] = ints[i-1] + diffs[(i-1) % diff_size];
  }
  return ints;
}


int *
decode_int_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
  return expand_diff_array(enc, tupdesc, out_size);
}

bool *
decode_bool_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
  bool isnull, *bools;
  Datum d_bools;
  int *ints;

  d_bools = heap_getattr(enc, 3, tupdesc, &isnull);
  if (!isnull) {
    ArrayType *ary = DatumGetArrayTypeP(d_bools);
    if (ARR_HASNULL(ary))
      elog(ERROR, "BOOLEAN[] contains null elements, raw pointer extraction unsafe.");
    *out_size = ArrayGetNItems(ARR_NDIM(ary), ARR_DIMS(ary));
    return (bool *) ARR_DATA_PTR(ary);
  }
  ints = expand_diff_array(enc, tupdesc, out_size);
  if (ints == NULL) {
    return NULL;
  }
  bools = palloc(*out_size * sizeof(bool));
  if (*out_size == 0) {
    return bools;
  }
  for (int i = 0; i != *out_size; i++) {
    if (ints[i] != 0 && ints[i] != 1) {
      elog(ERROR, "Invalid bool encoding: diff array contains values not in {0, 1}");
    } else {
      bools[i] = (bool)ints[i];
    }
  }
  return bools;
}

char **
decode_text_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size) {

  bool isnull, *nulls;
  Datum d_texts, *elements;
  ArrayType *ary;
  char **words, **base_words;
  int num_words, *ints;

  *out_size = 0;

  d_texts = heap_getattr(enc, 4, tupdesc, &isnull);
  if (isnull) {
    return NULL;
  }
  ary = DatumGetArrayTypeP(d_texts);

  deconstruct_array(ary, TEXTOID, -1, false, 'i', &elements, &nulls, &num_words);
  base_words = (char **) palloc(num_words * sizeof(char *));
  for (int i=0; i != num_words; i++) {
    if (!nulls[i]) {
      base_words[i] = TextDatumGetCString(elements[i]);
    } else {
      base_words[i] = NULL;
    }
  }

  words = (char **) palloc(*out_size * sizeof(char *));
  ints = expand_diff_array(enc, tupdesc, out_size);
  if (ints == NULL) {
    return NULL;
  }
  for (int i=0; i != *out_size; i++) {
    if (ints[i] < 0 || ints[i] >= num_words) {
      elog(ERROR, "Invalid text encoding: diff array contains out-of-bounds values");
    }
    words[i] = base_words[ints[i]];
  }
  return words;
}

/*
 * Expresses vals such that:
 * base = vals[0]
 * vals[i] = vals[i-1] + diff[(i-1) % len(diff)]
 * Uses KMP algorithm for finding smallest subsequence of diff which repeats
 * for the whole diff array.
 */
static void
encode_diff_array(int *vals, int vals_size, int **diff_buf, int *diff_size) {
  int *diffs, *p, k;
  int tmp_diff_size = vals_size - 1;

  *diff_buf = (int *) palloc(tmp_diff_size * sizeof(int));
  diffs = *diff_buf;

  if (vals_size == 0) {
    *diff_size = 0;
    return;
  }
  for (int i = 0; i != tmp_diff_size; i++) {
    diffs[i] = vals[i+1] - vals[i];
  }

  p = (int *) palloc0(tmp_diff_size * sizeof(int));
  k = 0;

  for (int i = 1; i != tmp_diff_size; i++) {
    while (k > 0 && diffs[i] != diffs[k])
      k = p[k-1];
    if (diffs[i] == diffs[k])
      k += 1;
    p[i] = k;
  }
  *diff_size = tmp_diff_size - p[tmp_diff_size-1];
  *diff_buf = repalloc((*diff_buf), *diff_size * sizeof(int));

}

static Datum
ints_to_datum(int *vals, int num_vals) {
  Datum *d_ints;
  ArrayType *ary;

  d_ints = (Datum *) palloc(sizeof(Datum) * num_vals);
  for (int i = 0; i != num_vals; i++) {
    d_ints[i] = Int32GetDatum(vals[i]);
  }
  ary = construct_array(d_ints, num_vals, INT4OID, 4, true, 'i');
  return PointerGetDatum(ary);
}


PG_FUNCTION_INFO_V1(encode_int_enc);

Datum
encode_int_enc(PG_FUNCTION_ARGS) {

  TupleDesc tupdesc;
  HeapTuple tuple;
  Datum values[7];
  bool nulls[7] = { false, true, true, true, false, false, false };
  ArrayType *int_ary;
  int num_vals, *vals, *diff, num_diffs;

  fdt_cache_init();

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
         errmsg("function returning record call in context "
           "that cannot accept a record type")));
  }

  BlessTupleDesc(tupdesc);

  int_ary = PG_GETARG_ARRAYTYPE_P(0);
  if (ARR_NDIM(int_ary) != 1)
    ereport(ERROR,
        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
         errmsg("encode_int_enc: input must be 1-D array")));
  if (ARR_HASNULL(int_ary))
    ereport(ERROR,
        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
         errmsg("encode_int_enc: array must not contain any NULL elements")));

  num_vals = ARR_DIMS(int_ary)[0];
  vals = (int *) ARR_DATA_PTR(int_ary);
  
  encode_diff_array(vals, num_vals, &diff, &num_diffs);

  values[0] = ObjectIdGetDatum(fdt_cache.label_oid[0]);
  values[4] = Int32GetDatum(vals[0]);
  values[5] = ints_to_datum(diff, num_diffs);
  values[6] = Int32GetDatum(num_vals);

  tuple = heap_form_tuple(tupdesc, values, nulls);
  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

