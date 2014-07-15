/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "internal.h"
#include "options.h"
#include "status.h"
#include "cache.h"
#include "cursor.h"
#include "tree.h"
#include "tree-func.h"
#include "txnmgr.h"
#include "logger.h"
#include "db.h"

#define DB_NAME ("ness.DB")

struct nessdb {
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;
	struct txnmgr *txnmgr;
	struct logger *logger;
};

struct nessdb *db_open(const char *basedir) {
	char dbpath[FILE_NAME_MAXLEN];
	struct nessdb *db;
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;
	struct txnmgr *txnmgr;

	ness_check_dir(basedir);
	memset(dbpath, 0, FILE_NAME_MAXLEN);
	snprintf(dbpath,
	         FILE_NAME_MAXLEN,
	         "%s/%s",
	         basedir,
	         DB_NAME);

	ness_file_exist(dbpath);

	opts = options_new();
	status = status_new();

	cache = cache_new(opts);
	if (!cache) {
		__PANIC("%s",
		        "create dbcache error, will exit...");
	}

	static struct tree_callback tree_cb = {
		.fetch_node = fetch_node_callback,
		.flush_node = flush_node_callback,
		.fetch_hdr = fetch_hdr_callback,
		.flush_hdr = flush_hdr_callback
	};
	tree = tree_open(dbpath, opts, status, cache, &tree_cb);
	if (!tree) {
		__PANIC("%s",
		        "create tree error, will exit...");
	}
	txnmgr = txnmgr_new();

	db = xcalloc(1, sizeof(*db));
	db->opts = opts;
	db->status = status;
	db->cache = cache;
	db->tree = tree;
	db->txnmgr = txnmgr;

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

int env_set_cache_size(struct nessdb *db, uint64_t cache_size)
{

	mutex_lock(&db->cache->mtx);
	db->opts->cache_limits_bytes = cache_size;
	mutex_unlock(&db->cache->mtx);

	return NESS_OK;
}

int env_set_compress_method(struct nessdb *db, ness_compress_method_t method)
{
	db->opts->compress_method = method;

	return NESS_OK;
}

int db_close(struct nessdb *db)
{
	cache_free(db->cache);
	tree_free(db->tree);
	options_free(db->opts);
	status_free(db->status);
	txnmgr_free(db->txnmgr);
	xfree(db);

	return 1;
}
