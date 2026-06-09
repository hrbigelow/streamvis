MODULE_big = streamvis_udfs

OBJS = window_avg.o enc_typ_codecs.o 

EXTENSION = streamvis_udfs

DATA = streamvis_udfs--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
