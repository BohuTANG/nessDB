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

SVR_OBJS = \
	./server/ae.o \
	./server/anet.o \
	./server/request.o \
	./server/response.o \
	./server/zmalloc.o \

TEST_OBJS = \
	./test/test_dump_and_restore.o
		
LIBRARY = libnessdb.a

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f $(LIB_OBJS)
	-rm -f $(SVR_OBJS)
	-rm -f $(TEST_OBJS)
	-rm -f bench/db-bench.o server/db-server.o
	-rm -f db-bench db-server db-test

cleandb:
	-rm -rf ndbs

$(LIBRARY): $(LIB_OBJS)
	rm -f $@
	$(AR) -rs $@ $(LIB_OBJS)

db-bench: bench/db-bench.o $(LIB_OBJS)
	$(CC)  bench/db-bench.o $(LIB_OBJS)  -o $@

db-server: server/db-server.o $(SVR_OBJS) $(LIB_OBJS)
	$(CC)  server/db-server.o $(SVR_OBJS) $(LIB_OBJS) -o $@

db-test: test/test_dump_and_restore.o $(LIB_OBJS)
	$(CC)  test/test_dump_and_restore.o $(LIB_OBJS)  -o $@

