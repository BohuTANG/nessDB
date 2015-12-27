MAJOR=3
MINOR=1
PLATFORM_LDFLAGS=-pthread
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -W -Wall -Werror -std=c99

ifdef ENABLE_ASAN
		WITH_SAN = -fsanitize=address -fno-omit-frame-pointer
else
		ifdef ENABLE_TSAN
		WITH_SAN = -fsanitize=thread
else
		WITH_SAN =
endif
endif

CC = clang-3.5
OPT ?= -O2 -DERROR# (A) Production use (optimized mode)
#OPT ?= -g2 -DINFO -DASSERT  -DUSE_VALGRIND # (B) Debug mode, w/ full line-level debugging symbols
#OPT ?= -O2 -g -DDEBUG -DASSERT# (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------

DIRS = ../../tree\
       ../../cache\
       ../../util\
       ../../log\
       ../../txn\
       ../../db\
       ../../include\
       ../
INCLUDES = $(addprefix -I, $(DIRS))
CFLAGS =  $(INCLUDES) $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT) $(WITH_SAN)

U_SRCS=$(wildcard ../../util/*.c)
U_OBJS=$(patsubst %.c,%.o,$(U_SRCS))

LIB_OBJS = $(T_OBJS)\
           $(U_OBJS)\
           $(X_OBJS)\
           $(C_OBJS)\
           $(L_OBJS)\
           $(D_OBJS)

TEST = test
TEST_OBJS = xtable-test.o\
            pma-test.o\
          main.o

all: banner $(TEST) $(LIB_OBJS)
banner:
	@echo "nessDB $(MAJOR).$(MINOR) -_-"
	@echo
	@echo "cc: $(CC)"
	@echo "cflags: $(CFLAGS)"
	@echo

clean:
	@rm -rf $(LIB_OBJS)
	@rm -rf $(TEST_OBJS)
	@rm -rf $(TEST)
	@rm -rf ness.event

$(TEST): banner $(TEST_OBJS) $(LIB_OBJS)
	$(CC) $(TEST_OBJS) $(PLATFORM_LDFLAGS) $(LIB_OBJS) $(WITH_SAN) -o $@

.PHONY: all banner clean
