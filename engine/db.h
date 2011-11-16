#ifndef _DB_H
#define _DB_H

#ifdef __cplusplus
extern "C" {
#endif

#include "storage.h"
#define DB_SLOT		(13)

struct info{
	int32_t used;
	int32_t unused;
};
typedef struct nessDB {
	struct btree _btrees[DB_SLOT];
	struct info _infos[DB_SLOT];
} nessDB;


/*
 * bufferpool_size:lru memory size allowed, unit is BYTE
 */
nessDB *db_init(int bufferpool_size, const char *basedir);
void   *db_get(nessDB *db, const char *key);
int    db_exists(nessDB *db, const char *key);
int    db_add(nessDB *db, const char *key,const char *value);
void   db_remove(nessDB *db, const char *key);
void   db_info(nessDB *db, char *infos);
void   db_destroy(nessDB *db);


#ifdef __cplusplus
}
#endif

#endif
