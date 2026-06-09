#ifndef _ENC_TYP_CACHE_H
#define _ENC_TYP_CACHE_H

#include "postgres.h"

typedef enum {
  FDT_INT = 0,
  FDT_FLOAT,
  FDT_TEXT,
  FDT_BOOL,
  FDT_NLABELS
} FdtLabel;

typedef struct {
  Oid type_oid;
  Oid label_oid[FDT_NLABELS];
  bool valid;
} FdtCache;

extern FdtCache fdt_cache;

extern void fdt_cache_init(void);

#endif // _ENC_TYP_CACHE_H
