#include <stdio.h>
#include <stdlib.h>

typedef struct io
{
	char		*buf;
	unsigned int	pos;
	unsigned int	buflen;
}io_t;
unsigned int next_power(unsigned int x);
io_t* io_new(unsigned int reserve);
void io_putc(io_t* io,char c);
void io_puti(io_t* io,int i);
void io_putl(io_t* io,long l);
void io_put(io_t*,char* buffer,int n);
char* io_detach(io_t* io);
unsigned int io_len(io_t* io);

int		io_geti(io_t* io);
char	io_getc(io_t* io);
char*	io_get(io_t*,int len);


