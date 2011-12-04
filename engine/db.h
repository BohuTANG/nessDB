#ifndef _DB_H
#define _DB_H

#include "util.h"

#include "llru.h"
#include "log.h"
#include "skiplist.h"

#define NESSDB_VERSION "1.8.1"
#define NESSDB_FEATURES "LSM-Tree && B+Tree with Level-LRU,Page-Cache"

struct nessdb {
	struct btree *btree;
	struct skiplist *list;
	struct log *log;
	struct llru lru;
};

struct nessdb *db_open(size_t bufferpool_size, const char *basedir);
void* db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
char* db_info(struct nessdb *db);
void db_flush(struct nessdb *db);
void db_close(struct nessdb *db);

#endif
