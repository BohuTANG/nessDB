#include "uthash.h"


typedef struct pointer
{
	unsigned int index;
	unsigned int offset;
	UT_hash_handle hh;
}pointer_t;


void 	vm_init();

int 	vm_put(char* key,char* value);
int 	vm_bulk_put(char* key,char* value);
void	vm_bulk_flush();
