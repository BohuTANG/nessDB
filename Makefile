# Detect OS
TARGET_OS := $(shell sh -c 'uname -s 2>/dev/null || echo not')

PLATFORM_FLAGS=
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -std=c99 -pedantic -W -Wall -Werror

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

CFLAGS = -Iengine -Idb $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(PLATFORM_FLAGS) $(OPT)

LIB_OBJS =	 			\
	./engine/compress/compress.o	\
	./engine/compress/quicklz.o	\
	./engine/hdrserialize.o	\
	./engine/serialize.o	\
	./engine/skiplist.o		\
	./engine/xmalloc.o		\
	./engine/atomic.o		\
	./engine/mempool.o		\
	./engine/basement.o		\
	./engine/posix.o		\
	./engine/crc32.o		\
	./engine/node.o		\
	./engine/tree.o		\
	./engine/tcursor.o		\
	./engine/block.o		\
	./engine/debug.o		\
	./engine/cpair.o		\
	./engine/file.o			\
	./engine/msg.o			\
	./engine/buf.o			\
	./db/dbcache.o			\
	./db/logw.o			\
	./db/logr.o			\
	./db/memtable.o			\
	./db/db.o			


BENCH_OBJS = \
	./bench/db-bench.o

LIBRARY = libbrt.so

all: $(LIBRARY)

clean:
	-rm -rf $(LIBRARY) $(LIB_OBJS) $(BENCH_OBJS) $(TEST) ness.event test.brt db-bench dbbench/

cleandb:
	-rm -rf dbbench/

$(LIBRARY): $(LIB_OBJS)
	$(CC) -pthread -fPIC -shared $(LIB_OBJS) -o $(LIBRARY) -lm

db-bench: $(BENCH_OBJS) $(LIB_OBJS)
	$(CC) -pthread $(LIB_OBJS) $(BENCH_OBJS) $(DEBUG) -o $@
