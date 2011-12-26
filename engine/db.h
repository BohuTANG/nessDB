/*
 * nessDB
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _DB_H
#define _DB_H

#include "index.h"
#include "util.h"

struct nessdb {
	struct index *idx;
};

struct nessdb *db_open(size_t bufferpool_size, const char *basedir, int tolog);
void *db_get(struct nessdb *db, struct slice *sk);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
void db_info(struct nessdb *db, char *infos);
void db_close(struct nessdb *db);

#endif
