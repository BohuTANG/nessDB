#ifndef _DB_H
#define _DB_H

#include "config.h"
#include <time.h>

#ifdef __cplusplus
	extern "C" {
#endif

struct nessdb {
	struct index *idx;
};

struct nessdb *db_open(const char *basedir);
int db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
char *db_info(struct nessdb *db);
void db_close(struct nessdb *db);

#ifdef __cplusplus
}
#endif
#endif
