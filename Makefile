CC = gcc

BGMERGE = -DBGMERGE
DEBUG =	-g -ggdb -DINFO

#detect OS,support Linux and Mac OS
UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	CFLAGS =-c -std=c99 -W -Wall -Werror -fPIC $(DEBUG) $(BGMERGE)
	LDFLAGS=-fPIC -shared
	LIB_EXTENSION=so
endif
ifeq ($(UNAME), Darwin)
	CFLAGS =-c -std=c99 -W -Wall -Werror $(DEBUG) $(BGMERGE)
	LDFLAGS=-std=c99 -W -Wall -Werror -dynamiclib -flat_namespace
	LIB_EXTENSION=dylib
endif




LIB_OBJS = \
	./engine/xmalloc.o\
	./engine/debug.o\
	./engine/sst.o\
	./engine/meta.o\
	./engine/buffer.o\
	./engine/index.o\
	./engine/quicklz.o\
	./engine/block.o\
	./engine/db.o

TEST = \
	./bench/db-bench.o


EXE = \
	./db-bench\

LIBRARY = libnessdb.$(LIB_EXTENSION)

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f $(LIB_OBJS)
	-rm -f $(EXE)
	-rm -f $(TEST)
	cd test;make clean

cleandb:
	-rm -rf ndbs
	-rm -rf *.event

$(LIBRARY): $(LIB_OBJS)
	$(CC) -pthread $(LDFLAGS) $(LIB_OBJS) -o libnessdb.$(LIB_EXTENSION) -lm

db-bench:  bench/db-bench.o $(LIB_OBJS)
	$(CC) -pthread $(LIB_OBJS) $(DEBUG) bench/db-bench.o -o $@ -lm
test: all $(LIB_OBJS)
	cd test;make
	export LD_LIBRARY_PATH=. && ./test/test
