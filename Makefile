UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	DEBUG = -g -ggdb -DDEBUG -fPIC
else
	DEBUG =	-g -ggdb -DDEBUG
endif

CC = clang
CFLAGS = -std=c99 -O3 -Wall -Wextra $(DEBUG)	

LIB_OBJS = \
	./engine/ht.o		\
	./engine/llru.o		\
	./engine/level.o	\
	./engine/db.o		\
	./engine/util.o		\
	./engine/skiplist.o \
	./engine/log.o		\
	./engine/buffer.o	\
	./engine/storage.o

SVR_OBJS = \
	./server/ae.o \
	./server/anet.o \
	./server/request.o \
	./server/response.o \
	./server/zmalloc.o \

BENCH_OBJS = ./bench/db-bench.c

LIBRARY = libnessdb.a

ALL = $(LIBRARY) db-bench db-server db-zmq mod-nessdb.so mod-skiplist.so mod-ht.so mod-null.so

all: $(ALL)

clean:
	-rm -f $(ALL)
	-rm -f bench/*.o server/*.o engine/*.o

cleandb:
	-rm -rf ndbs

$(LIBRARY): $(LIB_OBJS)
	rm -f $@
	$(AR) -rs $@ $(LIB_OBJS)
	rm -f $(LIB_OBJS)

.PHONY: BENCHMARK
BENCHMARK: db-bench
	./db-bench -k 32 -e 5000000 rwmix > $@

db-server: server/db-server.o $(SVR_OBJS:.o=.c) $(LIB_OBJS:.o=.c)
	$(CC) $(CFLAGS) -o $@ $+

db-bench: bench/db-bench.c server/db-zmq.c $(LIB_OBJS:.o=.c)
	$(CC) $(CFLAGS) -o $@ $+ -ldl

db-zmq: server/db-zmq.c $(LIB_OBJS:.o=.c)
	$(CC) $(CFLAGS) -DDBZ_MAIN -o $@ $+ -lzmq -ldl

mod-nessdb.so: server/mod-nessdb.c server/sha1.c $(LIB_OBJS:.o=.c)
	$(CC) $(CFLAGS) -shared -o $@ $+

mod-skiplist.so: ./engine/skiplist.c server/mod-skiplist.c
	$(CC) $(CFLAGS) -shared -o $@ $+

mod-ht.so: ./engine/ht.c server/mod-ht.c
	$(CC) $(CFLAGS) -shared -o $@ $+

mod-null.so: server/mod-null.c
	$(CC) $(CFLAGS) -shared -o $@ $+
