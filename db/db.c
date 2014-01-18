/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "internal.h"
#include "options.h"
#include "xmalloc.h"
#include "status.h"
#include "tcursor.h"
#include "dbcache.h"
#include "tree.h"
#include "memtable.h"
#include "db.h"

#define DB_NAME ("ness.DB")

struct nessdb {
	uint64_t msn;
	void *has_imm;
	struct options *opts;
	struct status *status;
	struct cache *cache;
	struct tree *tree;
	struct memtable *imm;
	struct memtable *mem;

	ness_mutex_t mem_mtx;
	ness_cond_t condvar;
	struct cron *compact_cron;
};

void _write_memtable(struct tree *tree, struct basement *bsm)
{
	struct basement_iter iter;

	basement_iter_init(&iter, bsm);
	basement_iter_seektofirst(&iter);
	while (basement_iter_valid(&iter)) {
		basement_iter_next(&iter);
		tree_put(tree, &iter.key, &iter.val, iter.type);
	}
}

void *_bg_compaction(void *arg)
{
	struct memtable *imm;
	struct nessdb *db = (struct nessdb*)arg;

	mutex_lock(&db->mem_mtx);
	imm = db->imm;
	mutex_unlock(&db->mem_mtx);
	if (imm != NULL) {
		compaction_begin(db->cache->extra);
		_write_memtable(db->tree, imm->bsm);

		mutex_lock(&db->mem_mtx);
		mtb_unref(imm);

		/* TODO: check there is no reference to this memtable before we free it */
		mtb_free(imm);
		db->imm = NULL;
		mutex_unlock(&db->mem_mtx);
		cond_signalall(&db->condvar);

		/*TODO: todo checkpoint in compaction finish */
		compaction_finish(db->cache->extra);
	}

	return NULL;
}

struct nessdb *db_open(const char *basedir)
{
	int exist;
	char dbpath[FILE_NAME_MAXLEN];
	struct nessdb *db;
	struct options *opts;
	struct status *status;
	struct cache *dbcache;
	struct tree *tree;

	ness_check_dir(basedir);
	memset(dbpath, 0, FILE_NAME_MAXLEN);
	snprintf(dbpath,
			FILE_NAME_MAXLEN,
			"%s/%s",
			basedir,
			DB_NAME);

	exist = ness_file_exist(dbpath);

	opts = options_new();
	status = status_new();

	dbcache = dbcache_new(opts);
	if (!dbcache) {
		__PANIC("%s",
			"create dbcache error, will exit...");
	}
	__WARN("%s", "create dbcache OK");

	if (exist)
		tree = tree_new(dbpath, opts, status, dbcache, 0);
	else
		tree = tree_new(dbpath, opts, status, dbcache, 1);
	if (!tree) {
		__PANIC("%s",
			"create tree error, will exit...");
	}
	__WARN("%s", "create tree OK");

	db = xcalloc(1, sizeof(*db));
	db->opts = opts;
	db->status = status;
	db->cache = dbcache;
	db->tree = tree;

	/* memtable */
	db->mem = mtb_new(db->opts, db->msn++);
	mtb_ref(db->mem);

	/* compaction cron */
	mutex_init(&db->mem_mtx);
	cond_init(&db->condvar);
	db->compact_cron = cron_new(_bg_compaction, 100);
	if (cron_start(db->compact_cron, (void*)db) != NESS_OK) {
		__PANIC("%s", "cron start error, will exit...");
	}
	__WARN("%s", "open database OK");

	return db;
}

void _make_room_for_write(struct nessdb *db)
{
	while (1) {
		if (mtb_memusage(db->mem) < db->opts->write_buffer_size_bytes) {
			return;
		} else {

			mutex_lock(&db->mem_mtx);
			if (db->imm != NULL) {
				mutex_unlock(&db->mem_mtx);
				cond_wait(&db->condvar);
			} else {
				struct memtable *new_mem;

				new_mem = mtb_new(db->opts, db->msn++);
				mtb_ref(new_mem);
				db->imm = db->mem;
				db->mem = new_mem;
				mutex_unlock(&db->mem_mtx);
				cron_signal(db->compact_cron);
				return;
			}
		}
	}
}

int db_set(struct nessdb *db, struct msg *k, struct msg *v)
{
	int r;

	//_make_room_for_write(db);
	//r = mtb_put(db->mem, k, v, MSG_PUT);
	r = tree_put(db->tree, k, v, MSG_PUT);

	return r;
}

int db_del(struct nessdb *db, struct msg *k)
{
	int r;

	_make_room_for_write(db);
	r = tree_put(db->tree, k, NULL, MSG_DEL);

	return r;
}

int db_get(struct nessdb *db, struct msg *k, struct msg *v)
{
	(void)db;
	(void)k;
	(void)v;

	return -1;
}

struct db_cursor *db_cursor_new(struct nessdb *db)
{
	struct db_cursor *cursor;

	cursor = xcalloc(1, sizeof(*cursor));
	cursor->db = db;

	/* if we have memtable basement on the fly
	 * we should add them to ancestor list first
	 */

	/*
	ancestors_append(tree_cursor, memtable0);
	ancestors_append(tree_cursor, memtablea);
	*/

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
	cron_free(db->compact_cron);
	if (db->mem) {
		_write_memtable(db->tree, db->mem->bsm);
		mtb_free(db->mem);
	}

	dbcache_free(db->cache);
	tree_free(db->tree);
	options_free(db->opts);
	status_free(db->status);
	xfree(db);

	return 1;
}
