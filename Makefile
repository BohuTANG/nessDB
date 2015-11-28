MAJOR=3
MINOR=1
PLATFORM_LDFLAGS=-pthread
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -W -Wall -Werror -std=c99

ifdef ENABLE_ASAN
WITH_SAN = -fsanitize=address -fno-omit-frame-pointer
else
 ifdef ENABLE_TSAN
 WITH_SAN = -fsanitize=thread
 else
 WITH_SAN =
 endif
endif

CC = gcc
#OPT ?= -O2 -DERROR# (A) Production use (optimized mode)
#OPT ?= -g2 -DINFO -DASSERT  -DUSE_VALGRIND # (B) Debug mode, w/ full line-level debugging symbols
OPT ?= -O2 -g -DDEBUG -DASSERT# (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------
INCLUDES =  -Iinclude -Itree -Icache -Iutil -Ilog -Itxn -Idb
CFLAGS =  $(INCLUDES) $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT) $(WITH_SAN)

LIB_OBJS =	 				\
	./tree/compress.o	\
	./tree/rolltree-func.o		\
	./tree/buftree-func.o		\
	./tree/rolltree.o		\
	./tree/buftree.o		\
	./tree/msgpack.o		\
	./tree/flusher.o		\
	./tree/cursor.o			\
	./tree/layout.o			\
	./tree/block.o			\
	./tree/inter.o			\
	./tree/node.o			\
	./tree/leaf.o			\
	./tree/hdr.o			\
	./tree/nmb.o			\
	./tree/lmb.o			\
	./tree/mb.o				\
	./util/snappy.o		\
	./util/comparator.o		\
	./util/xmalloc.o		\
	./util/mempool.o		\
	./util/kibbutz.o		\
	./txn/rollback.o		\
	./util/xtable.o		\
	./util/counter.o		\
	./util/posix.o			\
	./util/crc32.o			\
	./util/debug.o			\
	./util/quota.o		\
	./util/file.o			\
	./util/vfs.o			\
	./util/pma.o			\
	./util/msg.o			\
	./txn/txnmgr.o			\
	./txn/txn.o				\
	./cache/cache.o			\
	./log/logger.o			\
	./db/env.o				\
	./db/ness.o				\
	./db/db.o


BENCH_OBJS = \
	./bench/random.o \
	./bench/db-bench.o

LIBRARY = libnessdb.so

all: banner $(LIBRARY)
banner:
	@echo "nessDB $(MAJOR).$(MINOR) -_-"
	@echo
	@echo "cc: $(CC)"
	@echo "cflags: $(CFLAGS)"
	@echo

clean:
	-rm -rf $(LIBRARY) $(LIB_OBJS) $(BENCH_OBJS) $(TEST)
	-rm -rf dbbench db-bench ness.event

$(LIBRARY): banner $(LIB_OBJS)
	$(CC) $(PLATFORM_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(LIB_OBJS)  -shared -o $(LIBRARY)

db-bench: banner $(BENCH_OBJS) $(LIB_OBJS)
	$(CC) $(INCLUDES) -o $@ $(WITH_SAN) $(PLATFORM_LDFLAGS) $(LIB_OBJS) $(BENCH_OBJS)

.PHONY: all banner clean
