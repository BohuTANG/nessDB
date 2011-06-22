#include "uthash.h"


typedef struct pointer
{
	unsigned int index;
	unsigned int offset;
	UT_hash_handle hh;
}pointer_t;


void vm_init();

int vm_put(char* key,char* value);
int vm_bluk_put(char* key,char* value);
