UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	DEBUG = -g -rdynamic -ggdb 
else
	DEBUG =	-g -ggdb 
endif

CC = gcc
CFLAGS =  -c -std=c99 -Wall $(DEBUG)	

LIB_OBJS = \
	./engine/ht.o \
	./engine/llru.o \
	./engine/level.o \
	./engine/db.o \
	./engine/util.o \
	./engine/storage.o 

SVR_OBJS = \
	./server/ae.o \
	./server/anet.o \
	./server/request.o \
	./server/response.o \
	./server/zmalloc.o \
	$(LIB_OBJS)

LIBRARY = libnessdb.a

all: $(LIBRARY)

clean:
	-rm -rf $(LIBRARY)  $(LIB_OBJS) $(SVR_OBJS) ndbs bench/nessdb-bench.o server/nessdb-server.o db-bench db-server

$(LIBRARY): $(LIB_OBJS)
	rm -f $@
	$(AR) -rs $@ $(LIB_OBJS)

db-bench: bench/nessdb-bench.o $(LIB_OBJS)
	$(CC)  bench/nessdb-bench.o $(LIB_OBJS)  -o $@

db-server: server/nessdb-server.o $(SVR_OBJS)
	$(CC)  server/nessdb-server.o $(SVR_OBJS)  -o $@

#db-test: test/test_different_dirs.o $(SVR_OBJS)
#	$(CC)  test/test_different_dirs.o $(SVR_OBJS)  -o $@

