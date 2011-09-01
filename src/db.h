#ifndef _DB_H
#define _DB_H

//if lru_maxnum<=0,LRU closed
void 	db_init(int lru_maxnum);

void*	db_get(char* key);
int 	db_add(char* key,char* value);
void	db_remove(char* key);
void 	db_destroy();

#endif
