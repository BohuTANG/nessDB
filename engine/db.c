 /* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of struct nessdb nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef _GNU_SOURCE
	#define _GNU_SOURCE
#endif
#ifndef _LARGEFILE64_SOURCE
	#define _LARGEFILE64_SOURCE
#endif

#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#ifdef _WIN32
	#include <direct.h>
#endif

#include "storage.h"
#include "debug.h"
#include "platform.h"
#include "db.h"

#define DB_PREFIX 	"ndbs/ness"
#define DB_DIR    	"ndbs"
#define LIST_SIZE	(2500000)


struct nessdb *db_open(size_t bufferpool_size, const char *basedir)
{
	char pre[256];
	const char *dir;
	struct nessdb *db;

	memset(pre, 0, 256);
	dir = concat_paths(basedir, DB_DIR);
	_ensure_dir_exists(dir);
	free((char*) dir);

 	
	db = malloc(sizeof(struct nessdb));
	db->btree = malloc(sizeof(struct btree));
	snprintf(pre, sizeof(pre), "%s/%s", basedir, DB_PREFIX);
	btree_init(db->btree, pre);
	
	db->list = skiplist_new(LIST_SIZE);
	db->log = log_new(pre);

	/*llru*/
	llru_init(&db->llru, bufferpool_size);

	return db;
}


int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	struct btree *btree = db->btree;
	struct skiplist *list = db->list;
	struct log *log = db->log;
	uint64_t data_offset;
	uint8_t opt = 1;

	data_offset = btree_insert_data(btree, sv->data, sv->len);
	log_append(log, sk, data_offset, opt);

	if(data_offset == 0)
		return 0;

	if (!skiplist_notfull(db->list)) {
		__DEBUG("%s, mtable-size:<%d>", "WARNING: Start merging", LIST_SIZE);

		int i;
		struct skipnode *x = list->hdr->forward[0];
		for (i = 0; i < list->count; i++) {
			if (x->opt == ADD)
				btree_insert_index(btree, x->key, x->val);
			else { 
				llru_remove(&db->llru, x->key);
				btree_delete(btree, x->key);
			}

			x = x->forward[0];
		}

		skiplist_free(list);

		list = skiplist_new(LIST_SIZE);
		db->list = list;

		/* Log truncate*/
		log_trunc(log);

		__DEBUG("%s", "WARNING: Finished merging");
	}

	skiplist_insert(list, sk, data_offset, ADD);
	llru_remove(&db->llru, sk->data);

	return (1);
}

void  *db_get(struct nessdb *db, struct slice *sk)
{
	void *val;
	struct slice *sv;
	struct skipnode *snode;
	struct btree *btree = db->btree;

	val = llru_get(&db->llru, sk->data);

	/* 1)Data is in Level-LRU, return*/
	if(val) {
		return val;
	}
	
	snode = skiplist_lookup(db->list, sk);

	/* 2)Data is mtable, goto it and get the offset*/
	if (snode) {
		/* If OPT is DEL, return NULL*/
		if (snode->opt == DEL)
			return NULL;

		/* 3)Get from on-disk B+tree index*/
		sv = btree_get_data(btree, snode->val);
		if (sv) {
			/* 4)Add to Level-LRU*/
			char *k_clone = calloc(1, sk->len);
			char *v_clone = calloc(1, sv->len);

			memcpy(k_clone, sk->data, sk->len);
			memcpy(v_clone, sv->data, sv->len);

			llru_set(&db->llru, k_clone, v_clone, sk->len, sv->len);
			val = sv->data;

			free(sv);
		}
	}
	return val;
}

int db_exists(struct nessdb *db, struct slice *sk)
{
	char *val = db_get(db, sk);
	if(val)
		return 1;

	return 0;
}

void db_remove(struct nessdb *db, struct slice *sk)
{
	struct btree *btree = db->btree;
	struct skiplist *list = db->list;
	struct log *log = db->log;

	/* Add to log*/
	log_append(log, sk, 0UL, 0);

	if (!skiplist_notfull(db->list)) {
		int i;
		struct skipnode *x = list->hdr->forward[0];
		for (i = 0; i < list->count; i++) {
			if (x->opt == ADD)
				btree_insert_index(btree, x->key, x->val);
			else { 
				llru_remove(&db->llru, x->key);
				btree_delete(btree, x->key);
			}

			x = x->forward[0];
		}

		skiplist_free(list);

		list = skiplist_new(LIST_SIZE);
		db->list = list;

		/* Log truncate*/
		log_trunc(log);
	}

	skiplist_insert(list, sk, 0UL, ADD);
	llru_remove(&db->llru, sk->data);
}


void db_info(struct nessdb *db, char *infos)
{
	/*TODO*/
}

void db_flush(struct nessdb *db)
{
	int i;
	struct btree *btree = db->btree;
	struct skiplist *list = db->list;
	struct log *log = db->log;
	struct skipnode *x = list->hdr->forward[0];

	for (i = 0; i < list->count; i++) {
		if (x->opt == ADD)
			btree_insert_index(btree, x->key, x->val);
		else { 
			llru_remove(&db->llru, x->key);
			btree_delete(btree, x->key);
		}

		x = x->forward[0];
	}
	skiplist_free(list);

	list = skiplist_new(LIST_SIZE);
	db->list = list;

	/* Log truncate*/
	log_trunc(log);
}

void db_close(struct nessdb *db)
{
	db_flush(db);
	__DEBUG("%s" ,"WARNING: Finished flushing mtable's datas to disk indices...");

	log_free(db->log);
	skiplist_free(db->list);
	btree_close(db->btree);
	free(db->btree);
	llru_free(&db->llru);
	free(db);
}
