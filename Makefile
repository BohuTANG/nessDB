PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -std=c99 -pedantic -W -Wall -Werror -D_GNU_SOURCE

CC = gcc
#OPT ?= -O2 -DERROR # (A) Production use (optimized mode)
#OPT ?= -g2 -DINFO -DASSERT # (B) Debug mode, w/ full line-level debugging symbols
OPT ?= -O2 -g2 -DWARN# (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------
INCLUDES = -Iengine -Idb
CFLAGS =  $(INCLUDES) $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT)

LIB_OBJS =	 			\
	./engine/compress/compress.o	\
	./engine/compress/snappy.o	\
	./engine/compare-func.o		\
	./engine/tree-func.o		\
	./engine/hdrse.o		\
	./engine/se.o			\
	./engine/skiplist.o		\
	./engine/xmalloc.o		\
	./engine/atomic.o		\
	./engine/mempool.o		\
	./engine/msgbuf.o		\
	./engine/fifo.o 		\
	./engine/posix.o		\
	./engine/crc32.o		\
	./engine/block.o		\
	./engine/debug.o		\
	./engine/cpair.o		\
	./engine/node.o			\
	./engine/cache.o		\
	./engine/tree.o			\
	./engine/leaf.o			\
	./engine/cursor.o		\
	./engine/file.o			\
	./engine/msg.o			\
	./engine/buf.o			\
	./engine/logw.o			\
	./engine/logr.o			\
	./engine/txnmgr.o		\
	./engine/logger.o		\
	./engine/txn.o			\
	./engine/rollback.o		\
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
