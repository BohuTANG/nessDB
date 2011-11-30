UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	DEBUG = -g -rdynamic -ggdb -DDEBUG
else
	DEBUG =	-g -ggdb -DDEBUG
endif

CC = gcc
CFLAGS =  -c -std=c99 -Wall -Wextra -pedantic $(DEBUG)	

LIB_OBJS = \
	./engine/ht.o		\
	./engine/llru.o		\
	./engine/level.o	\
	./engine/db.o		\
	./engine/util.o		\
	./engine/skiplist.o		\
	./engine/log.o		\
	./engine/buffer.o	\
	./engine/storage.o 

SVR_OBJS = \
	./server/ae.o \
	./server/anet.o \
	./server/request.o \
	./server/response.o \
	./server/zmalloc.o \

TEST_ENGINE_OBJS = \
	./test/engine.o \
	./test/engine_llru.o

LIBRARY = libnessdb.a

all: $(LIBRARY) db-bench db-server

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
	./db-bench add > $@

db-server: server/db-server.o $(SVR_OBJS) $(LIB_OBJS)
	$(CC)  server/db-server.o $(SVR_OBJS) $(LIB_OBJS) -o $@

db-bench: bench/db-bench.o $(LIB_OBJS)
	$(CC)  bench/db-bench.o $(LIB_OBJS) -o $@

test-engine: $(TEST_ENGINE_OBJS)
	$(CC) -o $@ $+ -lcheck $(LIBRARY)
