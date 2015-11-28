/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "c.h"
#include "t.h"
#include "x.h"

#define DB_NAME ("ness.DB")

struct nessdb {
	struct env *e;
	struct buftree *tree;
};

struct nessdb *db_open(struct env *e, const char *dbname)
{
	char dbpath[FILE_NAME_MAXLEN];
	struct nessdb *db;
	struct buftree *tree;

	memset(dbpath, 0, FILE_NAME_MAXLEN);
	snprintf(dbpath,
	         FILE_NAME_MAXLEN,
	         "%s/%s",
	         e->dir,
	         dbname);
	ness_file_exist(dbpath);
	tree = buftree_open(dbpath, e->cache);

	db = xcalloc(1, sizeof(*db));
	db->e = e;
	db->tree = tree;

	return db;
}

int db_set(struct nessdb *db, struct msg *k, struct msg *v)
{
	int r;

	r = buftree_put(db->tree, k, v, MSG_INSERT, NULL);

	return r;
}

int db_del(struct nessdb *db, struct msg *k)
{
	int r;

	r = buftree_put(db->tree, k, NULL, MSG_DELETE, NULL);

	return r;
}

int db_get(struct nessdb *db, struct msg *k, struct msg *v)
{
	(void)db;
	(void)k;
	(void)v;

	return -1;
}

int db_change_compress_method(struct nessdb *db, int method)
{
	if (method >= (int)sizeof(ness_compress_method_t))
		return NESS_ERR;

	buftree_set_compress_method(db->tree->hdr, method);

	return NESS_OK;
}

int db_close(struct nessdb *db)
{
	buftree_free(db->tree);
	xfree(db);

	return 1;
}
