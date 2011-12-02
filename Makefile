UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	DEBUG = -g -ggdb -DDEBUG -fPIC
else
	DEBUG =	-g -ggdb -DDEBUG
endif

CC = gcc
CFLAGS = -std=c99 -Wall -Wextra $(DEBUG)	

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

ZMQ_OBJS = \
	./server/db-zmq.o

BENCH_OBJS = ./bench/db-bench.c

LIBRARY = libnessdb.a

all: $(LIBRARY) db-bench db-server db-zmq mod-nessdb.so

clean:

	-rm -f $(LIBRARY)  
	-rm -f $(LIB_OBJS)
	-rm -f $(SVR_OBJS)
	-rm -f bench/db-bench.o server/db-server.o 
	-rm -f db-bench db-server

cleandb:
	-rm -rf ndbs

$(LIBRARY): $(LIB_OBJS)
	rm -f $@
	$(AR) -rs $@ $(LIB_OBJS)

.PHONY: BENCHMARK
BENCHMARK: db-bench
	./db-bench -k 32 -e 5000000 rwmix > $@

db-server: server/db-server.o $(SVR_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) -o $@ $+

db-bench: $(BENCH_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) -o $@ $+

db-zmq: $(ZMQ_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) -o $@ $+ -lzmq -ldl

mod-nessdb.so: $(LIB_OBJS) server/mod-nessdb.c
	$(CC) $(CFLAGS) -shared -o $@ $+

cachegrind: db-bench
	valgrind --tool=cachegrind ./db-bench rwmix
