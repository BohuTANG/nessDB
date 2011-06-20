CC=gcc
CFLAGS=-c -Wall -O3

all: test

test:db.o  ivm.o main.o
	$(CC) ivm.o main.o -o test
db.o:db.c
	$(CC) $(CFLAGS) -fpic db.c
ivm.o: ivm.c db.c
	$(CC) $(CFLAGS) -fpic ivm.c db.c

main.o: main.c
	$(CC) $(CFLAGS) main.c

clean:
	rm -rf *.o test



