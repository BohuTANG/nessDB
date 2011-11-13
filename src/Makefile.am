AM_CFLAGS = -std=c99
lib_LTLIBRARIES = libnessDB.la
libnessDB_la_SOURCES = db.c storage.c ht.c level.c llru.c
libnessDB_la_LDFLAGS = -version-info 0:0:0
include_HEADERS = db.h
bin_PROGRAMS = nessdb-server nessdb-bench
nessdb_server_SOURCES = ../x/ae.c ../x/anet.c ../x/zmalloc.c request.c response.c nessdb-server.c
nessdb_server_LDADD = libnessDB.la
nessdb_bench_SOURCES = nessdb-bench.c
nessdb_bench_LDADD = libnessDB.la