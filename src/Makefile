CC = gcc 

CFLAGS =	-O2  -g


#name all the object files
OBJS = nessdb_bench.o db.o lru.o storage.o bloom.o

all : nessdb_bench

nessdb_bench : $(OBJS)
	$(CC) $(CFLAGS)  -lrt -o $@ $(OBJS)

clean :
	rm -rf $(OBJS) nessdb_bench

cleanall : 
	rm -rf $(OBJS) nessdb_bench ness.*
