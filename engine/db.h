#ifndef _DB_H
#define _DB_H

#include "util.h"

#include "llru.h"
#include "log.h"
#include "skiplist.h"

struct nessdb {
	struct btree *btree;
	struct skiplist *list;
	struct log *log;
	llru_t llru;
};

struct nessdb *db_open(size_t bufferpool_size, const char *basedir);
void *db_get(struct nessdb *db, struct slice *sk);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
void db_info(struct nessdb *db, char *infos);
void db_destroy(struct nessdb *db);

#endif
