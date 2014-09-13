PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -std=c99 -W -Wall -Werror

CC = gcc
# OPT ?= -O2 -DERROR # (A) Production use (optimized mode)
 OPT ?= -g2 -DINFO -DASSERT # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DERROR # (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------

top_builddir = ../..
INCLUDES =  -I$(top_builddir)/include -I$(top_builddir)/tree -I$(top_builddir)/cache -I$(top_builddir)/util -I$(top_builddir)/log -I$(top_builddir)/txn -I$(top_builddir)/db

CFLAGS =  $(INCLUDES) $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT)

LIB_OBJS =	 			\
	$(top_builddir)/tree/compress/compress.o	\
	$(top_builddir)/tree/compress/snappy.o		\
	$(top_builddir)/tree/tree-func.o		\
	$(top_builddir)/tree/flusher.o			\
	$(top_builddir)/tree/msgpack.o			\
	$(top_builddir)/tree/block.o			\
	$(top_builddir)/tree/layout.o			\
	$(top_builddir)/tree/hdr.o			\
	$(top_builddir)/tree/node.o			\
	$(top_builddir)/tree/tree.o			\
	$(top_builddir)/tree/leaf.o			\
	$(top_builddir)/tree/nmb.o			\
	$(top_builddir)/tree/lmb.o			\
	$(top_builddir)/tree/msg.o			\
	$(top_builddir)/tree/mb.o			\
	$(top_builddir)/util/comparator.o		\
	$(top_builddir)/util/xmalloc.o			\
	$(top_builddir)/util/mempool.o			\
	$(top_builddir)/util/kibbutz.o			\
	$(top_builddir)/util/posix.o			\
	$(top_builddir)/util/crc32.o			\
	$(top_builddir)/util/file.o			\
	$(top_builddir)/util/debug.o			\
	$(top_builddir)/util/pma.o			\
	$(top_builddir)/util/counter.o			\
	$(top_builddir)/txn/txnmgr.o			\
	$(top_builddir)/txn/txn.o			\
	$(top_builddir)/txn/rollback.o			\
	$(top_builddir)/cache/cache.o			\
	$(top_builddir)/db/db.o


TEST_OBJS = 						\
	counter-test.o					\
	main.o			


test: $(LIB_OBJS) $(TEST_OBJS)
		$(CC) -pthread $(LIB_OBJS) $(TEST_OBJS) $(OPT) -o $@

clean:
	-rm -rf $(LIB_OBJS) $(TEST_OBJS) test
	-rm -rf *.brt ness.event
