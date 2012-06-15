CC = gcc
DEBUG =	-g -ggdb -DDEBUG
CFLAGS =  -c -std=c99 -W -Wall -Werror -fPIC  $(DEBUG)

LIB_OBJS = \
	./engine/db.o\
	./engine/sst.o\
	./engine/log.o\
	./engine/util.o\
	./engine/meta.o\
	./engine/debug.o\
	./engine/index.o\
	./engine/bloom.o\
	./engine/buffer.o\
	./engine/skiplist.o

SVR_OBJS = \
	./server/ae.o\
	./server/anet.o\
	./server/request.o\
	./server/response.o\
	./server/zmalloc.o

LIBRARY = libnessdb.so

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f db-bench db-server
	-rm -f bench/db-bench.o server/db-server.o 
	-rm -f $(SVR_OBJS)
	-rm -f $(LIB_OBJS)

cleandb:
	-rm -rf ndbs
	-rm -rf *.event

$(LIBRARY): $(LIB_OBJS)
	$(CC) -pthread  -fPIC -shared $(LIB_OBJS) -o libnessdb.so

db-bench: bench/db-bench.o $(LIB_OBJS)
	$(CC) -pthread  bench/db-bench.o $(LIB_OBJS) -o $@

db-server: server/db-server.o $(SVR_OBJS) $(LIB_OBJS)
	$(CC) -pthread server/db-server.o $(SVR_OBJS) $(LIB_OBJS) -o $@

