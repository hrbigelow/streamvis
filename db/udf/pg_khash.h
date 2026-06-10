#ifndef _PG_KHASH_H
#define _PG_KHASH_H

#include "postgres.h"

#define kmalloc(s)      palloc(s)
#define kcalloc(n, s)   palloc0((n) * (s))
#define krealloc(p, s)  ((p) ? repalloc((p), (s)) : palloc(s))
#define kfree(p)        do { if (p) pfree(p); } while(0)

#include "khash.h"

#endif // _PG_KHASH_H
