CC = gcc 
CFLAGS = -Wall -g -std=c99

OBJS = ae.o  anet.o request.o response.o zmalloc.o
SVR_OBJS= svr.o

all :svr

svr : $(OBJS) $(SVR_OBJS)
	$(CC) $(CFLAGS) -o svr $^


%.o : %.c
	$(CC) $(CFLAGS) -o $@ -c $^


clean :
	rm -rf $(OBJS) $(SVR_OBJS) svr



