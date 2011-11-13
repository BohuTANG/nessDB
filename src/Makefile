UNAME := $(shell uname)

CC = gcc 
CFLAGS =-Wall -std=c99
ifeq ($(UNAME), Linux)
	DEBUG = -g -rdynamic -ggdb 
else
	DEBUG =	-g -ggdb 
endif

#name all the object files
LIB_OBJS = db.o  storage.o ht.o level.o llru.o 
TEST_OBJS = $(LIB_OBJS) nessdb-bench.o
SVR_OBJS =  $(LIB_OBJS) ../x/ae.o  ../x/anet.o ../x/zmalloc.o request.o response.o  nessdb-server.o

all : nessdb-bench nessdb-server
nessdb-bench : $(TEST_OBJS)
	$(CC) $(CFLAGS) $(DEBUG)  -o $@ $(TEST_OBJS) 

nessdb-server : $(SVR_OBJS)
	$(CC) $(CFLAGS) $(DEBUG)  -o $@ $(SVR_OBJS) 

clean :
	rm -rf $(TEST_OBJS) $(SVR_OBJS)  nessdb-bench nessdb-server libnessdb.so unit-test

cleanall : 
	rm -rf $(TEST_OBJS) $(SVR_OBJS)   nessdb-bench nessdb-server  ndbs/*.idx ndbs/*.db unit-test
