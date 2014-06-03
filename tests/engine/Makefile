top_builddir = ../..
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -std=c99 -pedantic -W -Wall -Werror -D_GNU_SOURCE

CC = gcc
# OPT ?= -O2 -DERROR # (A) Production use (optimized mode)
OPT ?= -g2 -DINFO -DASSERT # (B) Debug mode, $(top_builddir)/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DERROR # (C) Profiling mode: opt, but $(top_builddir)/debugging symbols
#-----------------------------------------------

CFLAGS = -I$(top_builddir)/engine -I$(top_builddir)/db $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT)

LIB_OBJS =	 			\
	$(top_builddir)/engine/compress/compress.o	\
	$(top_builddir)/engine/compress/snappy.o	\
	$(top_builddir)/engine/compare-func.o		\
	$(top_builddir)/engine/xmalloc.o		\
	$(top_builddir)/engine/mempool.o		\
	$(top_builddir)/engine/atomic.o		\
	$(top_builddir)/engine/posix.o		\
	$(top_builddir)/engine/crc32.o		\
	$(top_builddir)/engine/file.o			\
	$(top_builddir)/engine/buf.o			\
	$(top_builddir)/engine/debug.o		\
	$(top_builddir)/engine/array.o		\
	$(top_builddir)/engine/pma.o			\
	$(top_builddir)/engine/fifo.o 		\
	$(top_builddir)/engine/hdrse.o		\
	$(top_builddir)/engine/tree-func.o		\
	$(top_builddir)/engine/se.o			\
	$(top_builddir)/engine/msgbuf.o		\
	$(top_builddir)/engine/flusher.o		\
	$(top_builddir)/engine/block.o		\
	$(top_builddir)/engine/node.o			\
	$(top_builddir)/engine/tree.o			\
	$(top_builddir)/engine/leaf.o			\
	$(top_builddir)/engine/msg.o			\
	$(top_builddir)/engine/cache.o		\
	$(top_builddir)/engine/cpair.o		\
	$(top_builddir)/engine/logw.o			\
	$(top_builddir)/engine/logr.o			\
	$(top_builddir)/engine/txnmgr.o		\
	$(top_builddir)/engine/logger.o		\
	$(top_builddir)/engine/txn.o			\
	$(top_builddir)/engine/rollback.o		\

TEST_OBJS = 				\
	file-test.o			\
	block-test.o			\
	buf-test.o			\
	log-test.o			\
	compress-test.o			\
	msgbuf-test.o			\
	fifo-test.o			\
	tree-test.o			\
	hdr-serialize-test.o		\
	txn-test.o		\
	main.o			


test: ctest.h $(LIB_OBJS) $(TEST_OBJS)
		$(CC) -pthread $(LIB_OBJS) $(TEST_OBJS) $(OPT) $(DEBUG) -o $@

clean:
	-rm -rf $(LIB_OBJS) $(TEST_OBJS) test
	-rm -rf *.brt ness.event
