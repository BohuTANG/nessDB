CC = gcc 
LDFLAGS = 

# set NOCACHE options
ifdef NOCACHE
CFLAGS =  -O2  -DNOCACHE 
else
CFLAGS =  -O2 
endif

#name all the object files
OBJS = nessdb_bench.o vm.o hashtable.o

all : nessdb_bench

nessdb_bench : $(OBJS)
	$(CC) $(CFLAGS) -o nessdb_bench $^

nocache :
	make all NOCACHE=1

%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $^


clean :
	rm -rf $(OBJS) nessdb_bench

cleanall : 
	rm -rf $(OBJS) nessdb_bench ness.*
