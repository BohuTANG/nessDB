#include "uthash.h"

void 	vm_init();

int		vm_get(char* key,char* value);
int 	vm_put(char* key,char* value);
int 	vm_bulk_put(char* key,char* value);
void	vm_bulk_flush();
