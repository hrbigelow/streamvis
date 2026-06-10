#ifndef _SV_UTILS_H
#define _SV_UTILS_H

#include "postgres.h"
#include "utils/array.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"

TupleDesc
acquire_tupdesc(HeapTupleHeader rec);

HeapTupleData
wrap_header(HeapTupleHeader rec);

ArrayType * 
ints_to_array(int *vals, int num_vals);

ArrayType *
texts_to_array(const char **words, int num_vals);

ArrayType *
bools_to_array(bool *bools, int num_vals);

const char **
array_to_texts(ArrayType *ary, int *n);

int *
array_to_ints(ArrayType *ary, int *n);

bool *
array_to_bools(ArrayType *ary, int *n);

float *
array_to_floats(ArrayType *ary, int *n);

void
check_full_array(ArrayType *ary, int *num_vals, const char *where);

#endif // _SV_UTILS_H
