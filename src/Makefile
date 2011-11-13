UNAME := $(shell uname)

CC = gcc 
CFLAGS =-Wall -std=c99 -fPIC
LDLIBS = -pthread
ifeq ($(UNAME), Linux)
	DEBUG = -g -rdynamic -ggdb 
else
	DEBUG =	-g -ggdb 
endif

#name all the object files
LIB_OBJS = db.o  storage.o bloom.o ht.o level.o llru.o 
TEST_OBJS = $(LIB_OBJS) nessdb-bench.o
SVR_OBJS =  $(LIB_OBJS) ../x/ae.o  ../x/anet.o ../x/zmalloc.o request.o response.o  nessdb-server.o
SHARED_LIBRARY= libnessdb.so

all : nessdb-bench nessdb-server $(SHARED_LIBRARY)

nessdb-bench : $(TEST_OBJS)
	$(CC) $(CFLAGS) $(DEBUG) $(LDLIBS) -o $@ $(TEST_OBJS) 

nessdb-server : $(SVR_OBJS)
	$(CC) $(CFLAGS) $(DEBUG) $(LDLIBS) -o $@ $(SVR_OBJS) 

$(SHARED_LIBRARY): $(LIB_OBJS)
	$(CC) -shared $(LIB_OBJS) $(LDFLAGS) -o $@

test : $(TEST_OBJS)
	$(CC) $(CFLAGS) $(LDLIBS) $(LIB_OBJS) unit-test.c  -o unit-test
	./unit-test
	$(MAKE) cleanall


clean :
	rm -rf $(TEST_OBJS) $(SVR_OBJS)  nessdb-bench nessdb-server libnessdb.so unit-test

cleanall : 
	rm -rf $(TEST_OBJS) $(SVR_OBJS)   nessdb-bench nessdb-server  ndbs/*.idx ndbs/*.db unit-test
