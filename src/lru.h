#define MAX_CACHE_SIZE (1024*64)

char* 	lru_find(char* k);
void	lru_add(char* k,char* v);
