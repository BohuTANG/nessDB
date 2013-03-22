
CC = gcc

CFLAGS =  -c -std=c99 -W -Wall -Werror $(DEBUG) $(BGMERGE)
LDFLAGS = -L.. -lnessdb
INCPATH = -I..

ifdef CTEST_COLOR_OK
	CCFLAGS+=-DCOLOR_OK
endif

UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	LD_PATH=LD_LIBRARY_PATH
endif
ifeq ($(UNAME), Darwin)
	LDFLAGS +=-Wl,-flat_namespace,-undefined,dynamic_lookup
	LD_PATH=DYLD_LIBRARY_PATH
endif

all: test

%.o: %.c ctest.h
	$(CC) $(CCFLAGS) $(INCPATH) -c -o $@ $<

test: main.o ctest.h buffer-test.o db-test.o
	$(CC) $(LDFLAGS) main.o buffer-test.o db-test.o -o test
clean:
	rm -f test *.o

