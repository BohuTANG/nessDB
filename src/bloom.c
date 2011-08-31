#include <stdlib.h>
#include <stdarg.h>
#include "hashes.h"
#include "bloom.h"

#define HFUNCNUM 4
#define CHAR_BIT 8

#define SETBIT_1(bitset,i) (bitset[i / CHAR_BIT] |=  (1<<(i % CHAR_BIT)))
#define SETBIT_0(bitset,i) (bitset[i / CHAR_BIT] &=  (~(1<<(i % CHAR_BIT))))
#define GETBIT(bitset,i) (bitset[i / CHAR_BIT] &   (1<<(i % CHAR_BIT)))

void bloom_init(struct bloom *bloom,int size)
{
	bloom->size=size;
	bloom->bitset=calloc((size+1)/CHAR_BIT,sizeof(char));
	bloom->hashfuncs=malloc(HFUNCNUM*sizeof(hashfuncs));

	bloom->hashfuncs[0]=sax_hash;
	bloom->hashfuncs[1]=sdbm_hash;
	bloom->hashfuncs[2]=murmur_hash;
	bloom->hashfuncs[3]=jenkins_hash;
}

void bloom_add(struct bloom *bloom,const char *k)
{
	if(!k)
		return;

	int i;
	for(i=0;i<HFUNCNUM;i++)
	{
		int bit=bloom->hashfuncs[i](k)%bloom->size;
		SETBIT_1(bloom->bitset,bit);
	}
	bloom->count++;
}

int bloom_get(struct bloom *bloom,const char *k)
{
	if(!k)
		return 0;

	int i;
	for(i=0;i<HFUNCNUM;i++)
	{
		int bit=bloom->hashfuncs[i](k)%bloom->size;
		if(GETBIT(bloom->bitset,bit)==0)
			return -1;
	}
	return 0;
}

void bloom_free(struct bloom *bloom)
{
	if(bloom->bitset)
		free(bloom->bitset);
	
	if(bloom->hashfuncs)
		free(bloom->hashfuncs);
}

/*
int main()
{
	const char *k="yes";	
	struct bloom _b;
	bloom_init(&_b,1000);
	bloom_add(&_b,k);
	int ret=bloom_get(&_b,k);
	printf("kk:%d\n",ret);
	printf("kk:%d\n",bloom_get(&_b,"no"));
	return 1;
}
*/
