CC = gcc

#shrinks the size of db size
SHRINK = -DSHRINK 

#use background merge
BGMERGE = -DBGMERGE

#debug levle
DEBUG =	-g -O2 -ggdb -DINFO
CFLAGS =  -c -std=c99 -W -Wall -Werror -fPIC  $(DEBUG) $(BGMERGE) $(SHRINK)

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
	./engine/lru.o\
	./engine/compact.o\
	./engine/skiplist.o

TEST = \
	./bench/db-bench.o\
	./test/lru-test.o\
	./test/compact-test.o

EXE = \
	./db-bench\
	./lru-test\
	./compact-test

LIBRARY = libnessdb.so

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f $(LIB_OBJS)
	-rm -f $(EXE)
	-rm -f $(TEST)

cleandb:
	-rm -rf ndbs
	-rm -rf *.event

$(LIBRARY): $(LIB_OBJS)
	$(CC) -pthread  -fPIC -shared $(LIB_OBJS) -o libnessdb.so

db-bench:  $(LIB_OBJS) $(TEST)
	$(CC) -pthread  $(LIB_OBJS) bench/db-bench.o -o $@

lru-test:  $(LIB_OBJS) $(TEST)
	$(CC) -pthread  $(LIB_OBJS) test/lru-test.o -o $@

compact-test:  $(LIB_OBJS) $(TEST)
	$(CC) -pthread  $(LIB_OBJS) test/compact-test.o -o $@
