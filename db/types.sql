DROP TYPE IF EXISTS enc_typ;
/*
enc_typ represents an ordered 1D sequence of values, viewed
as a flattened tensor of shape `shape`, and the following logic:

Exactly one of i32_spans or f32_spans will be non-null.  I'll refer to this non-null
one as 'spans'.  The elements of shape and spans are parallel.

spans[dim] == null:  dim has no broadcasting or regular-increment (range) pattern.
spans[dim] != null (>= 0):  the values along dim are evenly spaced from orig[dim]
  to orig[dim] + spans[dim].  A span value of zero represents broadcasting.
base:  the flattened values of orig such that if orig repeats along dimension dim, base is
the zero-th slice of this dimension, otherwise, it is the full set of values.

Here, orig means the original tensor which is encoded by this scheme.

For detail, see client/streamvis/dbutil.py: encode_array, decode_array
*/

CREATE TYPE enc_typ AS (
  base BYTEA,
  shape INT[],
  i32_spans INT[], 
  f32_spans REAL[]
);
