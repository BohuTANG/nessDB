PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -W -Wall -Werror -std=c99

CC = gcc
#OPT ?= -O2 -DERROR# (A) Production use (optimized mode)
#OPT ?= -g2 -DINFO -DASSERT  -DUSE_VALGRIND# (B) Debug mode, w/ full line-level debugging symbols
OPT ?= -O2 -g2 -DDEBUG -DASSERT# (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------
INCLUDES =  -Iinclude -Itree -Icache -Iutil -Ilog -Itxn -Idb
CFLAGS =  $(INCLUDES) $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT)

LIB_OBJS =	 			\
	./tree/compress/compress.o	\
	./tree/compress/snappy.o	\
	./tree/tree-func.o		\
	./tree/cursor.o			\
	./tree/flusher.o		\
	./tree/msgpack.o		\
	./tree/layout.o			\
	./tree/block.o			\
	./tree/hdr.o			\
	./tree/node.o			\
	./tree/tree.o			\
	./tree/leaf.o			\
	./tree/nmb.o			\
	./tree/lmb.o			\
	./tree/msg.o			\
	./tree/mb.o			\
	./util/comparator.o		\
	./util/xmalloc.o		\
	./util/mempool.o		\
	./util/kibbutz.o		\
	./util/posix.o			\
	./util/crc32.o			\
	./util/file.o			\
	./util/debug.o			\
	./util/pma.o			\
	./util/counter.o		\
	./txn/txnmgr.o			\
	./txn/txn.o			\
	./txn/rollback.o		\
	./cache/cache.o			\
	./db/db.o


BENCH_OBJS = \
	./bench/random.o \
	./bench/db-bench.o

LIBRARY = libnessdb.so

all: $(LIBRARY)

clean:
	-rm -rf $(LIBRARY) $(LIB_OBJS) $(BENCH_OBJS) $(TEST) ness.event test.brt db-bench dbbench/

cleandb:
	-rm -rf dbbench/

$(LIBRARY): $(LIB_OBJS)
	$(CC) -pthread -fPIC -shared $(LIB_OBJS) -o $(LIBRARY) -lm

db-bench: $(BENCH_OBJS) $(LIB_OBJS)
	$(CC) -pthread $(LIB_OBJS) $(BENCH_OBJS) $(DEBUG) -o $@
