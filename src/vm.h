
void 	vm_init(int capacity);
void	vm_load_index();

int	    vm_get(char* key,char* value);
int 	vm_put(char* key,char* value);
void	vm_remove(char* key);
int 	vm_bulk_put(char* key,char* value);
void	vm_bulk_flush();
void 	vm_destroy();
