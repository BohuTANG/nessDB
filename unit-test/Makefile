CC = gcc 
CFLAGS =	-O2  -g -Werror

#CT unit test
TO_TEST_LIBS = ../src/bloom.c ../src/idx.c
TO_TEST_OBJS= $(TO_TEST_LIBS:.c=.o)

TESTS = $(wildcard *-test.c)
TOBJS = $(TESTS:.c=.o)

#CT unit test
.PHONY: check
check:  ct/_ctcheck
	ct/_ctcheck

ct/ct.o: ct/ct.h

$(tobjs): ct/ct.h

ct/_ctcheck: ct/_ctcheck.o ct/ct.o $(TO_TEST_OBJS) $(TOBJS)

ct/_ctcheck.c: $(TOBJS) ct/gen
	ct/gen $(TOBJS) > $@.part
	mv $@.part $@

clean :
	rm -rf ct/_* *.o ct/*.o
