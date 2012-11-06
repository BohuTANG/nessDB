/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "db.h"
#include "index.h"
#include "buffer.h"
#include "debug.h"
#include "xmalloc.h"

struct nessdb *db_open(const char *basedir)
{
	struct nessdb *db;

	db = xcalloc(1, sizeof(struct nessdb));
	db->idx = index_new(basedir, NESSDB_MAX_MTB_SIZE);

	return db;
}

STATUS db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	return index_add(db->idx, sk, sv);
}

STATUS db_get(struct nessdb *db, struct slice *sk, struct slice *sv)
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
