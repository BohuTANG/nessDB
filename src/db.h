#ifndef _DB_H
#define _DB_H

/*
 * bufferpool:lru memory size allow,unit is BYTE
 * isbgsync:if TRUE,"fdatasync" will run on the background thread,otherwise will do nothing.
 */
void 	db_init(int bufferpool_size,int isbgsync);

void*	db_get(const char* key);
int 	db_add(const char* key,const char* value);
void	db_remove(const char* key);
void 	db_destroy();

#endif
