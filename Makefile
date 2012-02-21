UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	DEBUG = -g -rdynamic -ggdb -DDEBUG
else
	DEBUG =	-g -ggdb -DDEBUG
endif

CC = gcc
CFLAGS =  -c -std=c99 -Wall  $(DEBUG)	

LIB_OBJS = \
	./engine/meta.o		\
	./engine/sst.o		\
	./engine/index.o	\
	./engine/db.o		\
	./engine/util.o		\
	./engine/skiplist.o		\
	./engine/log.o		\
	./engine/buffer.o	\
	./engine/ht.o		\
	./engine/level.o	\
	./engine/bloom.o	\
	./engine/llru.o		\
	./engine/debug.o

SVR_OBJS = \
	./server/ae.o \
	./server/anet.o \
	./server/request.o \
	./server/response.o \
	./server/zmalloc.o \

LIBRARY = libnessdb.so

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f $(LIB_OBJS)
	-rm -f $(SVR_OBJS)
	-rm -f bench/db-bench.o server/db-server.o 
	-rm -f db-bench db-server

cleandb:
	-rm -rf ndbs
	-rm -rf *.event

$(LIBRARY): $(LIB_OBJS)
	$(CC) -pthread  -fPIC -shared $(LIB_OBJS)  -o libnessdb.so

db-bench: bench/db-bench.o $(LIB_OBJS)
	$(CC) -pthread  bench/db-bench.o $(LIB_OBJS)  -o $@

db-server: server/db-server.o $(SVR_OBJS) $(LIB_OBJS)
	$(CC) -pthread server/db-server.o $(SVR_OBJS) $(LIB_OBJS) -o $@

