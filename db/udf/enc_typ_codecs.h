#ifndef _ENC_TYP_CODECS_H
#define _ENC_TYP_CODECS_H

#include "postgres.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"

TupleDesc
acquire_tupdesc(HeapTupleHeader rec);

HeapTupleData
wrap_header(HeapTupleHeader rec);

float *
decode_float_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

int *
decode_int_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

bool *
decode_bool_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);

char **
decode_text_enc(HeapTuple enc, TupleDesc tupdesc, int *out_size);


#endif // _ENC_TYP_CODECS_H
