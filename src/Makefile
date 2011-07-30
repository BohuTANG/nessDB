CC = gcc 
LDFLAGS = 

# set DEBUG options
ifdef DEBUG
CFLAGS = -Wall -Wextra -ggdb -pg -DDEBUG 
else
CFLAGS =  -O2 
endif

#name all the object files
OBJS = nessdb_bench.o vm.o hashtable.o

all : nessdb_bench

db_bench : $(OBJS)
	$(CC) $(CFLAGS) -o nessdb_bench $^

debug :
	make all DEBUG=1

%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $^


clean :
	rm -rf $(OBJS) nessdb_bench ness.*
