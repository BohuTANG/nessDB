/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "internal.h"
#include "posix.h"
#include "env.h"
#include "comparator.h"
#include "tree-func.h"
#include "tree.h"
#include "txnmgr.h"
#include "logger.h"
#include "db.h"

#define DB_NAME ("ness.DB")

struct nessdb {
	struct env *e;
	struct tree *tree;
};

struct env *env_open(const char *home, uint32_t flags) {
	struct env *e;

	e = xcalloc(1, sizeof(*e));
	e->flags = flags;

	/* tree */
	e->inner_node_fanout = 16;
	e->inner_default_node_size = 4 << 20;	/* 4MB */
	e->leaf_default_node_size = 1 << 20;		/* 1MB */
	e->leaf_default_basement_size = 128 << 10;	/* 128KB */

	/* cache */
	e->cache_limits_bytes = 1024 << 20;
	e->cache_high_watermark = 80;		/* 80% */
	e->cache_flush_period_ms = 100;		/* 0.1s */
	e->cache_checkpoint_period_ms = 600000;	/* 60s */

	/* IO */
	e->use_directio = 1;
	e->redo_path = "./dbbench";
	e->enable_redo_log = 1;
	if (!home)
		home = ".";

	e->dir = xcalloc(1, strlen(home) + 1);
	xmemcpy(e->dir, (void*)home, strlen(home));
	ness_check_dir(e->dir);

	/* compress */
	e->compress_method = NESS_SNAPPY_METHOD;

	/* callback */
	e->bt_compare_func = bt_compare_func_builtin;

	/* internal */
	e->cache = cache_new(e);
	nassert(e->cache);

	e->txnmgr = txnmgr_new();
	nassert(e->txnmgr);

	e->status = status_new();
	nassert(e->status);

	return e;
}

void env_close(struct env *e)
{
	cache_free(e->cache);
	status_free(e->status);
	txnmgr_free(e->txnmgr);
	xfree(e->dir);
	xfree(e);
}

struct nessdb *db_open(struct env *e, const char *dbname) {
	char dbpath[FILE_NAME_MAXLEN];
	struct nessdb *db;
	struct tree *tree;

	memset(dbpath, 0, FILE_NAME_MAXLEN);
	snprintf(dbpath,
	         FILE_NAME_MAXLEN,
	         "%s/%s",
	         e->dir,
	         dbname);
	ness_file_exist(dbpath);

	static struct tree_callback tree_cb = {
		.fetch_node_cb = tree_fetch_node_callback,
		.flush_node_cb = tree_flush_node_callback,
		.fetch_hdr_cb = tree_fetch_hdr_callback,
		.flush_hdr_cb = tree_flush_hdr_callback,
		.free_node_cb = tree_free_node_callback,
		.cache_put_cb = tree_cache_put_callback,
		.node_is_dirty_cb = tree_node_is_dirty_callback,
		.node_set_nondirty_cb = tree_node_set_nondirty_callback,
	};
	tree = tree_open(dbpath, e, &tree_cb);

	db = xcalloc(1, sizeof(*db));
	db->e = e;
	db->tree = tree;

	return db;
}

int db_set(struct nessdb *db, struct msg *k, struct msg *v)
{
	int r;

	r = tree_put(db->tree, k, v, MSG_INSERT, NULL);

	return r;
}

int db_del(struct nessdb *db, struct msg *k)
{
	int r;

	r = tree_put(db->tree, k, NULL, MSG_DELETE, NULL);

	return r;
}

int db_get(struct nessdb *db, struct msg *k, struct msg *v)
{
	(void)db;
	(void)k;
	(void)v;

	return -1;
}

int env_set_cache_size(struct env *e, uint64_t cache_size)
{
	e->cache_limits_bytes = cache_size;

	return NESS_OK;
}

int env_set_compress_method(struct env *e, int method)
{
	if (method >= (int)sizeof(ness_compress_method_t))
		return NESS_ERR;

	e->compress_method = method;

	return NESS_OK;
}

int env_set_compare_func(struct env *e, int (*bt_compare_func)(void *a, int lena, void *b, int lenb))
{
	e->bt_compare_func = bt_compare_func;

	return NESS_OK;
}

int db_close(struct nessdb *db)
{
	tree_free(db->tree);
	xfree(db);

	return 1;
}
