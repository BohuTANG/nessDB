#ifndef _DB_H
#define _DB_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * bufferpool:lru memory size allow,unit is BYTE
 * isbgsync:if TRUE,"fdatasync" will run on the background thread,otherwise will do nothing.
 */
void 	db_init(int bufferpool_size,int isbgsync);

void	*db_get(const char *key);

int 	db_add(const char *key,const char *value);
void	db_update(const char *key,const char *value);
void	db_remove(const char *key);
void	db_info(char *infos);
void 	db_destroy();


#ifdef __cplusplus
}
#endif

#endif
