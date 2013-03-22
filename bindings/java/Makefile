CC = gcc

DEBUG=-g  -ggdb -DINFO
#detect OS,support Linux and Mac OS
UNAME := $(shell uname)
ifeq ($(UNAME), Linux)
	CFLAGS =-c -std=c99 -W -Wall -Werror -fPIC $(DEBUG) $(BGMERGE)
	LDFLAGS=-fPIC -shared
	LIB_EXTENSION=so
endif
ifeq ($(UNAME), Darwin)
	CFLAGS =-c -std=c99 -W -Wall -Werror $(DEBUG) $(BGMERGE)
	LDFLAGS=-dynamiclib
	LIB_EXTENSION=dylib
endif
LIB_JNI_OBJS=\
	db_java.o	

LIB_OBJS=\
	../../engine/xmalloc.o\
	../../engine/debug.o\
	../../engine/sst.o\
	../../engine/meta.o\
	../../engine/buffer.o\
	../../engine/index.o\
	../../engine/quicklz.o\
	../../engine/block.o\
	../../engine/db.o

LIBRARY=libnessdb_jni.$(LIB_EXTENSION)

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f $(LIB_JNI_OBJS)

$(LIBRARY): $(LIB_OBJS) $(LIB_JNI_OBJS)
	$(CC) -pthread $(LDFLAGS) $^ -o $@ -lm
