CC = gcc 
CFLAGS = -Wall -g

OBJS = event.o ../src/ht.o   request.o  nessdb-x.o

all :x

x : $(OBJS)
	$(CC) $(CFLAGS) -o nessdb-x $^

%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $^


clean :
	rm -rf $(OBJS) nessdb-x



