CC = gcc

#debug levle
DEBUG =	-g -O2 -ggdb -DINFO
CFLAGS =  -c -std=c99 -W -Wall -Werror -fPIC  $(DEBUG) 

LIB_OBJS = \
	./engine/xmalloc.o\
	./engine/debug.o\
	./engine/sorts.o\
	./engine/cola.o\
	./engine/meta.o\
	./engine/buffer.o\
	./engine/index.o\
	./engine/db.o

TEST = \
	./bench/db-bench.o

EXE = \
	./db-bench

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
