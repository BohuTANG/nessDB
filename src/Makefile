CC = gcc 

# set NOCACHE options
ifdef NOCACHE
CFLAGS =	-g	  -DNOCACHE
else
CFLAGS =	-g
endif

#name all the object files
OBJS = nessdb_bench.o db.o hashtable.o btree.o

all : nessdb_bench

nessdb_bench : $(OBJS)
	$(CC) $(CFLAGS)  -lrt -o $@ $(OBJS)

nocache :
	make all NOCACHE=1

clean :
	rm -rf $(OBJS) nessdb_bench

cleanall : 
	rm -rf $(OBJS) nessdb_bench ness.*
