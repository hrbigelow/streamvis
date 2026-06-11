#include "postgres.h"
#include "enc_typ_core.h"
#include "enc_typ_cache.h"
#include "sv_utils.h"

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

float *
enc_typ_to_floats(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
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

int *
enc_typ_to_ints(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
  return expand_diff_array(enc, tupdesc, out_size);
}

bool *
enc_typ_to_bools(HeapTuple enc, TupleDesc tupdesc, int *out_size) {
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

const char **
enc_typ_to_texts(HeapTuple enc, TupleDesc tupdesc, int *out_size) {

  bool isnull, *dummy;
  Datum d_texts;
  ArrayType *ary;
  const char **words, **base_words;
  int num_words, *ints;

  *out_size = 0;

  d_texts = heap_getattr(enc, 4, tupdesc, &isnull);
  if (isnull) {
    return NULL;
  }
  ary = DatumGetArrayTypeP(d_texts);
  base_words = array_to_texts(ary, &dummy, &num_words);

  words = (const char **) palloc(*out_size * sizeof(char *));
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
void
encode_diff_array(int *vals, int vals_size, int **diff_buf, int *diff_size) {
  int *diffs, *p, k;
  int tmp_diff_size = vals_size - 1;

  *diff_buf = (int *) palloc(tmp_diff_size * sizeof(int));

  if (vals_size < 2) {
    *diff_size = tmp_diff_size;
    return;
  }

  diffs = *diff_buf;

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

/*
 */
void
decode_diff_array(int *diff, int diff_size, int base, int **vals, int vals_size) {
  *vals = (int *) palloc(vals_size * sizeof(int));
  if (vals_size == 0) return;
  (*vals)[0] = base;

  for (int i = 1; i != vals_size; i++) {
    (*vals)[i] = (*vals)[i-1] + diff[(i-1) % diff_size];
  }
}


