CC = gcc 
CFLAGS =	  -g -Wall  -fPIC

#name all the object files
TEST_OBJS = nessdb_bench.o
LIB_OBJS = db.o  storage.o bloom.o ht.o level.o llru.o

all : libnessdb.so nessdb_bench

nessdb_bench : $(TEST_OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS)  -lrt -pthread -o $@ $(TEST_OBJS) $(LIB_OBJS)

libnessdb.so : $(LIB_OBJS)
	$(CC) $(CFLAGS) $(LIB_OBJS) -shared -fPIC -o $@

clean :
	rm -rf $(TEST_OBJS) $(LIB_BJS)  nessdb_bench libnessdb.so

cleanall : 
	rm -rf $(TEST_OBJS) $(LIB_OBJS)  nessdb_bench libnessdb.so ndbs/*.idx ndbs/*.db
