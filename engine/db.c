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

#include "llru.h"
#include "log.h"
#include "skiplist.h"
#include "storage.h"
#include "debug.h"
#include "platform.h"
#include "db.h"

#define DB_PREFIX 	"ndbs/ness"
#define DB_DIR    	"ndbs"
#define LIST_SIZE	(1000000)


struct nessdb *db_open(size_t bufferpool_size, const char *basedir)
{
	char pre[256];
	struct nessdb *db;

	llru_init(bufferpool_size);
	memset(pre, 0, 256);
	_ensure_dir_exists(concat_paths(basedir, DB_DIR));
 	
	db = malloc(sizeof(struct nessdb));

	db->btree = malloc(sizeof(struct btree));
	snprintf(pre, sizeof(pre), "%s/%s", basedir, DB_PREFIX);
	btree_init(db->btree, pre);
	
	/*mtable*/
	db->list = skiplist_new(LIST_SIZE);

	/*log*/
	db->log = log_new(pre);

	return db;
}

int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	struct btree *btree = db->btree;
	struct skiplist *list = db->list;
	struct log *log = db->log;
	UINT data_offset;

	/*log*/
	log_append(log, sk, sv);

	data_offset = btree_insert_data(btree, sv->data, sv->len);
	if(data_offset == 0)
		return 0;

	if (!skiplist_notfull(db->list)) {
		/*TODO merge*/
		int i;
		struct skipnode *x = list->hdr->forward[0];
		for (i = 0; i < list->count; i++) {
			btree_insert_index(btree, x->key, x->val);
			x = x->forward[0];
		}

		skiplist_free(list);

		list = skiplist_new(LIST_SIZE);
		db->list = list;
	}

	skiplist_insert(list, sk, data_offset, ADD);
	llru_remove(sk->data);

	return (1);
}


void *db_get(struct nessdb *db, struct slice *sk)
{
	/*TODO*/
	return NULL;
}

int db_exists(struct nessdb *db, struct slice *sk)
{
	/*TODO*/
	return 0;
}

void db_remove(struct nessdb *db, struct slice *sk)
{
	/*TODO*/
	return;
}


void db_info(struct nessdb *db, char *infos)
{
	/*TODO*/
}

void db_destroy(struct nessdb *db)
{
	log_free(db->log);
	skiplist_free(db->list);
	btree_close(db->btree);
	llru_free();
	free(db);
}
