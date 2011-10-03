CC = gcc 
CFLAGS =	  -g -Wall 

#name all the object files
TEST_OBJS = db.o  storage.o bloom.o ht.o level.o llru.o nessdb_bench.o
LIB_OBJS = db.o  storage.o bloom.o ht.o level.o llru.o

all : nessdb_bench

nessdb_bench : $(TEST_OBJS)
	$(CC) $(CFLAGS)  -lrt -pthread -o $@ $(TEST_OBJS) 
	$(CC) $(CFLAGS) $(LIB_OBJS) -shared -o libnessdb.so

clean :
	rm -rf $(TEST_OBJS) $(LIB_BJS)  nessdb_bench libnessdb.so

cleanall : 
	rm -rf $(TEST_OBJS) $(LIB_OBJS)  nessdb_bench libnessdb.so ndbs/*.idx ndbs/*.db
