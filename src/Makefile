CC = gcc 
CFLAGS =	  -g -Wall 

#name all the object files
LIB_OBJS = db.o  storage.o bloom.o ht.o level.o llru.o 
TEST_OBJS = $(LIB_OBJS) nessdb_bench.o


all : nessdb_bench

nessdb_bench : $(TEST_OBJS)
	$(CC) $(CFLAGS)  -lrt -pthread -o $@ $(TEST_OBJS) 
	$(CC) $(CFLAGS) $(LIB_OBJS) -shared -fPIC -o libnessdb.so

test : $(TEST_OBJS)
	$(CC) $(CFLAGS)  -lrt -pthread $(LIB_OBJS) unit-test.c  -o unit-test
	./unit-test
	$(MAKE) cleanall


clean :
	rm -rf $(TEST_OBJS) $(LIB_BJS)  nessdb_bench libnessdb.so unit-test

cleanall : 
	rm -rf $(TEST_OBJS) $(LIB_OBJS)  nessdb_bench libnessdb.so ndbs/*.idx ndbs/*.db unit-test
