
void 	db_init(int capacity);
void	db_load_index();

int	    db_get(char* key,char* value);
int 	db_put(char* key,char* value);
void	db_remove(char* key);
int 	db_bulk_put(char* key,char* value);
void	db_bulk_flush();
void 	db_destroy();
