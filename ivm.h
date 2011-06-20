#include "uthash.h"


typedef struct pointer
{
	unsigned int index;
	unsigned int offset;
	UT_hash_handle hh;
}pointer_t;


void ivm_init();

int ivm_put(char* key,char* value);
int ivm_bluk_put(char* key,char* value);
