UNAME := $(shell uname)

CC = gcc 
CFLAGS =	  -g -Wall -fPIC

ifeq ($(UNAME), Linux)
LDLIBS = -lrt -lpthread
else
LDLIBS = -lpthread
endif

#name all the object files
LIB_OBJS = db.o  storage.o bloom.o ht.o level.o llru.o 
TEST_OBJS = $(LIB_OBJS) nessdb_bench.o
SVR_OBJS =  $(LIB_OBJS) ../x/ae.o  ../x/anet.o ../x/zmalloc.o request.o response.o  nessdb_server.o


all : nessdb-bench nessdb-server

nessdb-bench : $(TEST_OBJS)
	$(CC) $(CFLAGS) $(LDLIBS) -o $@ $(TEST_OBJS) 

nessdb-server : $(SVR_OBJS)
	$(CC) $(CFLAGS) $(LDLIBS) -o $@ $(SVR_OBJS) 


test : $(TEST_OBJS)
	$(CC) $(CFLAGS) $(LDLIBS) $(LIB_OBJS) unit-test.c  -o unit-test
	./unit-test
	$(MAKE) cleanall


clean :
	rm -rf $(TEST_OBJS) $(SVR_OBJS)  nessdb-bench nessdb-server libnessdb.so unit-test

cleanall : 
	rm -rf $(TEST_OBJS) $(SVR_OBJS)   nessdb-bench nessdb-server  ndbs/*.idx ndbs/*.db unit-test
