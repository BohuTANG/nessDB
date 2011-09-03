#ifndef _DB_H
#define _DB_H

void 	db_init();

void*	db_get(char* key);
int 	db_add(char* key,char* value);
void	db_remove(char* key);
void 	db_destroy();

#endif
