#ifndef _BLOOM_H
#define _BLOOM_H

struct bloom
{
	unsigned char	*bitset;
	unsigned int	size;
	unsigned int	count;
	unsigned int 	(*bloom_hashfunc) (const char*);
};

void	bloom_init(struct bloom *bloom,int  size);
void	bloom_add(struct bloom *bloom,const char *k);
int	bloom_get(struct bloom *bloom,const char *k);

#endif
