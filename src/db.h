
void 	db_init(int capacity,int lru);
void	db_load_index();

void*	db_get(char* key);
int 	db_add(char* key,char* value);
void	db_remove(char* key);
void 	db_destroy();
