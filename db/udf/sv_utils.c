#include "postgres.h"
#include "sv_utils.h"
#include "fmgr.h"
#include "access/tupdesc.h"
#include "access/htup_details.h"
#include "utils/typcache.h" 
#include "utils/array.h"
#include "utils/builtins.h"


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


ArrayType * 
ints_to_array(int *vals, int num_vals) {
  Datum *d_ints = (Datum *) palloc(sizeof(Datum) * num_vals);
  for (int i = 0; i != num_vals; i++) {
    d_ints[i] = Int32GetDatum(vals[i]);
  }
  return construct_array_builtin(d_ints, num_vals, INT4OID);
}

ArrayType *
floats_to_array(float *vals, int num_vals) {
  Datum * elems = (Datum *) palloc(sizeof(Datum) * num_vals);
  for (int i = 0; i != num_vals; i++) {
    elems[i] = Float4GetDatum(vals[i]);
  }
  return construct_array_builtin(elems, num_vals, FLOAT4OID);
}

ArrayType *
texts_to_array(const char **words, int num_vals) {
  Datum *d_words = (Datum *) palloc(sizeof(Datum) * num_vals);
  for (int i =0; i != num_vals; i++) {
    d_words[i] = CStringGetTextDatum(words[i]);
  }
  return construct_array_builtin(d_words, num_vals, TEXTOID);  
}

ArrayType *
bools_to_array(bool *bools, int num_vals) {
  Datum *d_bools = (Datum *) palloc(sizeof(Datum) * num_vals);
  for (int i = 0; i != num_vals; i++) {
    d_bools[i] = BoolGetDatum(bools[i]);
  }
  return construct_array_builtin(d_bools, num_vals, BOOLOID);
}

const char **
array_to_texts(ArrayType *ary, bool **nulls, int *n) {
  Datum *elements;
  const char **texts;

  deconstruct_array(ary, TEXTOID, -1, false, 'i', &elements, nulls, n);
  texts = (const char **) palloc(*n * sizeof(char *));
  for (int i=0; i != *n; i++) {
    if (!(*nulls)[i]) {
      texts[i] = TextDatumGetCString(elements[i]);
    } else {
      texts[i] = NULL;
    }
  }
  return texts;
}


int *
array_to_ints(ArrayType *ary, bool **nulls, int *n) {
  Datum *elems;
  int *vals;
  deconstruct_array_builtin(ary, INT4OID, &elems, nulls, n);
  vals = (int *) palloc(*n * sizeof(int));

  for (int i = 0; i != *n; i++) {
    if (!(*nulls)[i])
      vals[i] = DatumGetInt32(elems[i]);  
  }
  return vals;
}

bool *
array_to_bools(ArrayType *ary, bool **nulls, int *n) {
  Datum *elems;
  bool *vals;
  deconstruct_array(ary, BOOLOID, 1, true, 'c', &elems, nulls, n);
  vals = (bool *) palloc(*n * sizeof(bool));

  for (int i = 0; i != *n; i++) {
    if (!(*nulls)[i])
      vals[i] = DatumGetBool(elems[i]);  
  }
  return vals;
}

float *
array_to_floats(ArrayType *ary, bool **nulls, int *n) {
  Datum *elems;
  float *vals;
  deconstruct_array(ary, FLOAT4OID, sizeof(float), true, 'i', &elems, nulls, n);
  vals = (float *) palloc(*n * sizeof(float));

  for (int i = 0; i != *n; i++) {
    if (!(*nulls)[i])
      vals[i] = DatumGetFloat4(elems[i]);  
  }
  return vals;
}


void
check_full_array(ArrayType *ary, int *num_vals, const char *where) {
  if (ARR_NDIM(ary) != 1)
    ereport(ERROR,
        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
         errmsg("%s: input must be 1-D array", where)));
  if (ARR_HASNULL(ary))
    ereport(ERROR,
        (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
         errmsg("%s: array must not contain any NULL elements", where)));
  *num_vals = ARR_DIMS(ary)[0];
}
