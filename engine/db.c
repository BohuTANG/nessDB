/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include <stdlib.h>
#include <string.h>

#include "xmalloc.h"
#include "index.h"
#include "buffer.h"
#include "db.h"
#include "debug.h"

struct nessdb *db_open(const char *basedir)
{
	struct nessdb *db;

	db = xcalloc(1, sizeof(struct nessdb));
	db->idx = index_new(basedir, NESSDB_MAX_MTB_SIZE);

	return db;
}

int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	return index_add(db->idx, sk, sv);
}

int db_get(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	int ret;

	ret = index_get(db->idx, sk, sv);

	return ret;
}

void db_remove(struct nessdb *db, struct slice *sk)
{
	index_remove(db->idx, sk);
}

void db_close(struct nessdb *db)
{
	index_free(db->idx);
	free(db);
}
