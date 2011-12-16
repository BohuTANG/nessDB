#ifndef _DB_H
#define _DB_H

#include "index.h"
#include "util.h"

struct nessdb {
	struct index *idx;
};

struct nessdb *db_open(size_t bufferpool_size, const char *basedir);
void *db_get(struct nessdb *db, struct slice *sk);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
void db_info(struct nessdb *db, char *infos);
void db_flush(struct nessdb *db);
void db_close(struct nessdb *db);

#endif
