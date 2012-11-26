#ifndef __nessDB_H
#define __nessDB_H

#include "config.h"

#ifdef __cplusplus
	extern "C" {
#endif

struct nessdb {
	struct index *idx;
	struct stats *stats;
};

struct nessdb *db_open(const char *basedir);
STATUS db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
STATUS db_exists(struct nessdb *db, struct slice *sk);
STATUS db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_stats(struct nessdb *db, struct slice *infos);
void db_remove(struct nessdb *db, struct slice *sk);
void db_close(struct nessdb *db);

#ifdef __cplusplus
}
#endif
#endif
