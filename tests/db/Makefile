# Detect OS
TARGET_OS := $(shell sh -c 'uname -s 2>/dev/null || echo not')

top_builddir = ../..
PLATFORM_FLAGS=
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -std=c99 -Wall -Wno-unused-parameter


ifeq ($(TARGET_OS), Linux)
	PLATFORM_FLAGS=-DOS_LINUX
else ifeq ($(TARGET_OS), Darwin)
	PLATFORM_FLAGS=-DOS_MACOSX
else ifeq ($(TARGET_OS), OS_ANDROID_CROSSCOMPILE)
	PLATFORM_FLAGS=-DOS_ANDROID
else
	echo "Unknown platform!" >&2
	exit 1
endif

CC = gcc
# OPT ?= -O2 -DERROR # (A) Production use (optimized mode)
OPT ?= -g2 -DINFO -DASSERT # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DERROR # (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------

CFLAGS = -I$(top_builddir)/engine -I$(top_builddir)/db $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(PLATFORM_FLAGS) $(OPT)

LIB_OBJS =	 			\
	$(top_builddir)/engine/compress/compress.o	\
	$(top_builddir)/engine/compress/quicklz.o	\
	$(top_builddir)/engine/hdrserialize.o	\
	$(top_builddir)/engine/serialize.o	\
	$(top_builddir)/engine/skiplist.o	\
	$(top_builddir)/engine/xmalloc.o	\
	$(top_builddir)/engine/atomic.o		\
	$(top_builddir)/engine/mempool.o	\
	$(top_builddir)/engine/compare.o	\
	$(top_builddir)/engine/basement.o	\
	$(top_builddir)/engine/posix.o		\
	$(top_builddir)/engine/crc32.o		\
	$(top_builddir)/engine/block.o		\
	$(top_builddir)/engine/debug.o		\
	$(top_builddir)/engine/cpair.o		\
	$(top_builddir)/engine/node.o		\
	$(top_builddir)/engine/leaf.o		\
	$(top_builddir)/engine/tree.o		\
	$(top_builddir)/engine/tcursor.o	\
	$(top_builddir)/engine/file.o		\
	$(top_builddir)/engine/msg.o		\
	$(top_builddir)/engine/buf.o		\
	$(top_builddir)/db/dbcache.o		\
	$(top_builddir)/db/logw.o		\
	$(top_builddir)/db/logr.o		\
	$(top_builddir)/db/db.o			


TEST_OBJS = 				\
	log-test.o			\
	db-test.o			\
	main.o			


test: ctest.h $(LIB_OBJS) $(TEST_OBJS)
		$(CC) -pthread $(LIB_OBJS) $(TEST_OBJS) $(OPT) $(DEBUG) -o $@
		./test

clean:
	-rm -rf $(LIB_OBJS) $(TEST_OBJS) test
	-rm -rf *.brt ness.event ness.redo.*
