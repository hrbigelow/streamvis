#include "postgres.h"

#include "enc_typ_core.h"
#include "enc_typ_cache.h"
#include "sv_utils.h"

#include "fmgr.h"
#include "utils/array.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "pg_khash.h"

KHASH_MAP_INIT_STR(str, int);

PG_FUNCTION_INFO_V1(encode_int_enc);

Datum
encode_int_enc(PG_FUNCTION_ARGS) {

  TupleDesc tupdesc;
  HeapTuple tuple;
  Datum values[7];
  bool nulls[7] = { false, true, true, true, false, false, false };
  ArrayType *ary;
  int num_vals, *vals, *diff, num_diffs;

  fdt_cache_init();

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
         errmsg("function returning record call in context "
           "that cannot accept a record type")));
  }

  BlessTupleDesc(tupdesc);

  ary = PG_GETARG_ARRAYTYPE_P(0);

  check_full_array(ary, &num_vals, "encode_int_enc");
  vals = (int *) ARR_DATA_PTR(ary); 

  encode_diff_array(vals, num_vals, &diff, &num_diffs);

  values[ENC_TYP_FIELD_TYPE] = ObjectIdGetDatum(fdt_cache.label_oid[FDT_INT]);
  values[ENC_TYP_BASE] = Int32GetDatum(vals[0]);
  values[ENC_TYP_DIFF] = PointerGetDatum(ints_to_array(diff, num_diffs));
  values[ENC_TYP_SIZE] = Int32GetDatum(num_vals);

  tuple = heap_form_tuple(tupdesc, values, nulls);
  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

PG_FUNCTION_INFO_V1(encode_text_enc);


Datum
encode_text_enc(PG_FUNCTION_ARGS) {

  TupleDesc tupdesc;
  HeapTuple tuple;
  Datum *in_datum, enc_values[7];
  bool *in_null, enc_nulls[7] = { false, true, true, false, false, false, false };
  khash_t(str) *word_map = kh_init(str);
  const char *word, **words;
  ArrayType *ary;
  int num_vals, n_distinct_words = 0, *vals, num_diffs, *diff = NULL;

  fdt_cache_init();

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
         errmsg("function returning record call in context "
           "that cannot accept a record type")));
  }

  BlessTupleDesc(tupdesc);

  ary = PG_GETARG_ARRAYTYPE_P(0);
  check_full_array(ary, &num_vals, "encode_ext_enc");
  
  deconstruct_array(ary, TEXTOID, -1, false, TYPALIGN_INT, &in_datum, &in_null, &num_vals);
  vals = (int *) palloc(num_vals * sizeof(int));

  for (int i = 0; i != num_vals; i++) {
    int ret;
    khiter_t k;
    word = text_to_cstring(DatumGetTextPP(in_datum[i]));
    k = kh_put(str, word_map, word, &ret);
    if (ret == -1) {
      elog(ERROR, "failed to insert key \"%s\" into hash", word);
    } else if (ret == 0) {
      vals[i] = kh_val(word_map, k);
    } else {
      vals[i] = n_distinct_words++;
      kh_val(word_map, k) = vals[i];
    }
  }

  words = (const char **) palloc(n_distinct_words * sizeof(const char *));
  for (khiter_t k = kh_begin(word_map); k != kh_end(word_map); ++k) {
    int idx;
    if (!kh_exist(word_map, k)) continue;
    word = kh_key(word_map, k);
    idx = kh_val(word_map, k);
    words[idx] = word;
  }

  encode_diff_array(vals, num_vals, &diff, &num_diffs);

  enc_values[ENC_TYP_FIELD_TYPE] = ObjectIdGetDatum(fdt_cache.label_oid[FDT_TEXT]);
  enc_values[ENC_TYP_BASE] = Int32GetDatum(vals[0]);
  enc_values[ENC_TYP_TEXTS] = PointerGetDatum(texts_to_array(words, n_distinct_words));
  enc_values[ENC_TYP_DIFF] = PointerGetDatum(ints_to_array(diff, num_diffs));
  enc_values[ENC_TYP_SIZE] = Int32GetDatum(num_vals);

  tuple = heap_form_tuple(tupdesc, enc_values, enc_nulls);
  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

PG_FUNCTION_INFO_V1(encode_bool_enc);

Datum
encode_bool_enc(PG_FUNCTION_ARGS) {

  TupleDesc tupdesc;
  HeapTuple tuple;
  Datum enc_values[7];
  bool enc_nulls[7] = { true, true, true, true, true, true, true };
  ArrayType *ary;
  bool *bvals;
  int num_vals, *vals, num_diffs, *diff = NULL;

  fdt_cache_init();

  if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
         errmsg("function returning record call in context "
           "that cannot accept a record type")));
  }

  BlessTupleDesc(tupdesc);

  ary = PG_GETARG_ARRAYTYPE_P(0);
  bvals = array_to_bools(ary, &num_vals);
  vals = (int *) palloc(num_vals * sizeof(int));

  for (int i = 0; i != num_vals; i++) {
    vals[i] = (int) bvals[i];
  }

  encode_diff_array(vals, num_vals, &diff, &num_diffs);

  enc_nulls[ENC_TYP_FIELD_TYPE] = false;
  enc_values[ENC_TYP_FIELD_TYPE] = ObjectIdGetDatum(fdt_cache.label_oid[FDT_BOOL]);

  if (num_diffs * 4 < num_vals) {
    // worthwhile to store in compact form
    enc_nulls[ENC_TYP_BASE] = false;
    enc_nulls[ENC_TYP_DIFF] = false;
    enc_nulls[ENC_TYP_SIZE] = false;
    enc_values[ENC_TYP_BASE] = Int32GetDatum(vals[0]);
    enc_values[ENC_TYP_DIFF] = PointerGetDatum(ints_to_array(diff, num_diffs));
    enc_values[ENC_TYP_SIZE] = Int32GetDatum(num_vals);
  } else {
    enc_nulls[ENC_TYP_BOOLS] = false;
    enc_values[ENC_TYP_BOOLS] = PointerGetDatum(ary);
  }

  tuple = heap_form_tuple(tupdesc, enc_values, enc_nulls);
  PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

