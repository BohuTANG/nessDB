/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "internal.h"
#include "options.h"
#include "xmalloc.h"
#include "status.h"
#include "cache.h"
#include "tcursor.h"
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
	struct logger *logger;

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
	__WARN("%s", "create dbcache OK");

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
	__WARN("%s", "create tree OK");
	txnmgr = txnmgr_new();
	logger = logger_new(cache, txnmgr);

	db = xcalloc(1, sizeof(*db));
	db->opts = opts;
	db->status = status;
	db->cache = cache;
	db->tree = tree;
	db->txnmgr = txnmgr;
	db->logger = logger;
	__WARN("%s", "open database OK");

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

struct db_cursor *db_cursor_new(struct nessdb *db) {
	struct db_cursor *cursor;

	cursor = xcalloc(1, sizeof(*cursor));
	cursor->db = db;

	return cursor;
}

void db_cursor_free(struct db_cursor *cursor)
{
	cursor->valid = 0;
	xfree(cursor);
}

int db_c_valid(struct db_cursor *cursor)
{
	return (cursor->valid == 1);
}

void db_c_first(struct db_cursor *cursor)
{
	struct cursor *tree_cursor;

	assert(cursor->db == NULL);

	tree_cursor = cursor_new(cursor->db->tree);
	tree_cursor_first(tree_cursor);
	if (tree_cursor_valid(tree_cursor)) {
		cursor->valid = 1;
		cursor->key = tree_cursor->key;
		cursor->val = tree_cursor->val;
	} else
		cursor->valid = 0;
	cursor_free(tree_cursor);
}

void db_c_last(struct db_cursor *cursor)
{
	struct cursor *tree_cursor;

	assert(cursor->db == NULL);

	tree_cursor = cursor_new(cursor->db->tree);
	tree_cursor_last(tree_cursor);
	if (tree_cursor_valid(tree_cursor)) {
		cursor->valid = 1;
		cursor->key = tree_cursor->key;
		cursor->val = tree_cursor->val;
	} else
		cursor->valid = 0;
	cursor_free(tree_cursor);
}

void db_c_next(struct db_cursor *cursor)
{
	struct cursor *tree_cursor;

	assert(cursor->db == NULL);

	tree_cursor = cursor_new(cursor->db->tree);
	tree_cursor_next(tree_cursor);
	if (tree_cursor_valid(tree_cursor)) {
		cursor->valid = 1;
		cursor->key = tree_cursor->key;
		cursor->val = tree_cursor->val;
	} else
		cursor->valid = 0;
	cursor_free(tree_cursor);
}

void db_c_prev(struct db_cursor *cursor)
{
	struct cursor *tree_cursor;

	assert(cursor->db == NULL);

	tree_cursor = cursor_new(cursor->db->tree);
	tree_cursor_prev(tree_cursor);
	if (tree_cursor_valid(tree_cursor)) {
		cursor->valid = 1;
		cursor->key = tree_cursor->key;
		cursor->val = tree_cursor->val;
	} else
		cursor->valid = 0;
	cursor_free(tree_cursor);
}

int db_close(struct nessdb *db)
{
	cache_free(db->cache);
	tree_free(db->tree);
	options_free(db->opts);
	status_free(db->status);
	txnmgr_free(db->txnmgr);
	logger_free(db->logger);
	xfree(db);

	return 1;
}
