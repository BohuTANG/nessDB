/*
   io buffer 
   overred@gmail.com
 */
#include "io.h"
#include <stdlib.h>

static unsigned int
next_power(unsigned int x)
{
	--x;
	x|=x>>0x001;
	x|=x>>0x002;
	x|=x>>0x004;
	x|=x>>0x008;
	x|=x>>0x010;
	return (++x);
}

static void
io_fixed(io_t* io,int len)
{
	char* buf;
	len+=io->pos;
	if(len<io->buflen)
		return;

	if(!io->buflen)
		io->buflen=32;
	else
		io->buflen=next_power(len);

	buf=(char*)realloc(io->buf,io->buflen);
	io->buf=buf;
}

io_t*
io_new(unsigned int reserve)
{
	io_t* io=(io_t*)malloc(sizeof(io_t));
	io->buf=NULL;
	io->pos=0;
	io->buflen=0;

	if(reserve)
		io_fixed(io,reserve);
	return io;
}

void 
io_puti(io_t* io,int value)
{
	io_fixed(io,sizeof(int));
	io->buf[io->pos++]=(char)(value);
	io->buf[io->pos++]=(char)(value>>0x08);
	io->buf[io->pos++]=(char)(value>>0x10);
	io->buf[io->pos++]=(char)(value>>0x18);
}

void 
io_put(io_t* io,char* value,int len)
{
	int i=0;
	io_fixed(io,len);
	while(i<len)
	{
		io->buf[io->pos++]=value[i++];
	}
	
}

void 
io_putc(io_t* io,char value)
{
	io_fixed(io,1);
	io->buf[io->pos++]=value;
}

unsigned int 
io_len(io_t* io)
{
	return io->pos;
}

char*
io_detach(io_t* io)
{
	char* buf;
	buf=io->buf;
	io->buf=NULL;

	io->pos=0;
	io->buflen=0;
	return buf;
}

