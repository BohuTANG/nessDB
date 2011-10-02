CC = gcc 

CFLAGS =	  -g


#name all the object files
OBJS = nessdb_bench.o db.o  storage.o bloom.o ht.o level.o llru.o

all : nessdb_bench

nessdb_bench : $(OBJS)
	$(CC) $(CFLAGS)  -lrt -pthread -o $@ $(OBJS)

clean :
	rm -rf $(OBJS) nessdb_bench

cleanall : 
	rm -rf $(OBJS) nessdb_bench ndbs/*.idx ndbs/*.db
