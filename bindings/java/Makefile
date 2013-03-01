CC = gcc

DEBUG=-g  -ggdb -DINFO
CFLAGS=-std=c99 -W -Wall -Werror -fPIC $(DEBUG)
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

LIBRARY=libnessdb_jni.so

all: $(LIBRARY)

clean:
	-rm -f $(LIBRARY)  
	-rm -f $(LIB_JNI_OBJS)

$(LIBRARY): $(LIB_OBJS) $(LIB_JNI_OBJS)
	$(CC) -pthread -fPIC -shared $^ -o $@ -lm
