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
enc_typ_to_floats(HeapTuple enc, TupleDesc tupdesc, int *out_size);

int *
enc_typ_to_ints(HeapTuple enc, TupleDesc tupdesc, int *out_size);

bool *
enc_typ_to_bools(HeapTuple enc, TupleDesc tupdesc, int *out_size);

const char **
enc_typ_to_texts(HeapTuple enc, TupleDesc tupdesc, int *out_size);

void
encode_diff_array(int *vals, int vals_size, int **diff_buf, int *diff_size);

void
decode_diff_array(int *diff, int diff_size, int base, int **vals, int vals_size);

#endif // _ENC_TYP_CORE_H
