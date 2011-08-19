CC = gcc 
LDFLAGS = 

# set NOCACHE options
ifdef NOCACHE
CFLAGS =		-O2  -DNOCACHE 
else
CFLAGS =		-O2 
endif

ifdef DEBUG
CFLAGS =		-g
endif


#name all the object files
OBJS = nessdb_bench.o db.o hashtable.o btree.o

all : nessdb_bench

nessdb_bench : $(OBJS)
	$(CC) $(CFLAGS)	-o	$@ $(OBJS)

nocache :
	make all NOCACHE=1

debug	:	
	make all	DEBUG=1

clean :
	rm -rf $(OBJS) nessdb_bench

cleanall : 
	rm -rf $(OBJS) nessdb_bench ness.*
