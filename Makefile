MAJOR=3
MINOR=1
PLATFORM_LDFLAGS=-pthread
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_LDFLAGS=-c -W -Wall -Werror -std=gnu2x

ifdef ENABLE_ASAN
		WITH_SAN = -fsanitize=address -fno-omit-frame-pointer
else
		ifdef ENABLE_TSAN
		WITH_SAN = -fsanitize=thread
else
		WITH_SAN =
endif
endif

CC = gcc
#OPT ?= -O2 -DERROR# (A) Production use (optimized mode)
#OPT ?= -g2 -DINFO -DASSERT  -DUSE_VALGRIND # (B) Debug mode, w/ full line-level debugging symbols
OPT ?= -O2 -g -DDEBUG -DASSERT# (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------

DIRS = tree cache util log txn db include
INCLUDES = $(addprefix -I, $(DIRS))
CFLAGS =  $(INCLUDES) $(PLATFORM_SHARED_LDFLAGS) $(PLATFORM_SHARED_CFLAGS) $(OPT) $(WITH_SAN)

include $(addsuffix /m.dep, $(DIRS))
LIB_OBJS = $(T_OBJS)\
           $(U_OBJS)\
           $(X_OBJS)\
           $(C_OBJS)\
           $(L_OBJS)\
           $(D_OBJS)

include $(addsuffix /m.dep, bench)
BENCH_OBJS = $(B_OBJS)

LIBRARY = libnessdb.so
STATIC  = libnessdb.a
BENCH   = db-bench
all: banner $(LIB_OBJS) $(LIBRARY) $(STATIC)
banner:
	@echo "nessDB $(MAJOR).$(MINOR) -_-"
	@echo
	@echo "cc: $(CC)"
	@echo "cflags: $(CFLAGS)"
	@echo

clean:
	@rm -rf $(LIBRARY)
	@rm -rf $(STATIC)
	@rm -rf $(LIB_OBJS)
	@rm -rf $(BENCH_OBJS)
	@rm -rf $(BENCH)
	@rm -rf dbbench ness.event

$(LIBRARY): banner
	@echo "ld libnessdb.so"
	$(CC) $(LIB_OBJS) $(WITH_SAN) -shared -o $@

$(STATIC): banner
	@echo "ar libnessdb.a"
	@ar crs $@ $(LIB_OBJS)

$(BENCH): banner $(BENCH_OBJS) $(LIB_OBJS)
	$(CC) $(BENCH_OBJS) $(PLATFORM_LDFLAGS) $(LIB_OBJS) $(WITH_SAN) -o $@

.PHONY: all banner clean
