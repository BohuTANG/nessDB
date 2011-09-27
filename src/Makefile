CC = gcc 

CFLAGS =	  -g


#name all the object files
OBJS = nessdb_bench.o db.o idx.o storage.o bloom.o ht.o level.o llru.o

all : nessdb_bench

nessdb_bench : $(OBJS)
	$(CC) $(CFLAGS)  -lrt -o $@ $(OBJS)

clean :
	rm -rf $(OBJS) nessdb_bench

cleanall : 
	rm -rf $(OBJS) nessdb_bench ness.*
