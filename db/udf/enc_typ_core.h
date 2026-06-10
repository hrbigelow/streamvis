#ifndef _ENC_TYP_CORE_H
#define _ENC_TYP_CORE_H

#include "postgres.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"

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

float *
decode_float_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

int *
decode_int_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

bool *
decode_bool_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

const char **
decode_text_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

void
encode_diff_array(int *vals, int vals_size, int **diff_buf, int *diff_size);

#endif // _ENC_TYP_CORE_H
