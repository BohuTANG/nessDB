#include "uthash.h"
#ifndef __OBJ__
#define __OBJ__
typedef struct nessobj
{
	char* k;
	char* v;
}nessobj_t;
#endif

void 	vm_init(int lru);
void	vm_load_data();

int	vm_get(nessobj_t* obj);
int	vm_put(nessobj_t* obj);
int	vm_bulk_put(nessobj_t* obj);
void	vm_bulk_flush();
