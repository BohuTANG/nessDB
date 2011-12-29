 /*
 * nessDB
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */
 
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "llru.h"
#include "db.h"
#include "debug.h"

#define DB "ness"
#define LIST_SIZE	(5000000)


struct nessdb *db_open(size_t bufferpool_size, const char *basedir, int tolog)
{
	struct nessdb *db;
	 	
	db = malloc(sizeof(struct nessdb));
	db->idx = index_new(basedir, DB, LIST_SIZE, tolog);
	db->lru = llru_new(bufferpool_size);

	return db;
}


int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	llru_remove(db->lru, sk);
	return index_add(db->idx, sk, sv);
}

void *db_get(struct nessdb *db, struct slice *sk)
{
	char *data;
	struct slice *sv;
	sv = llru_get(db->lru, sk);
	if (sv) {
		__DEBUG("db get from llru,key:<%s>, data:<%s>", sk->data, sv->data);
		data = malloc(sv->len);
		memcpy(data, sv->data, sv->len);
	} else {
		struct slice sv_clone;

		data = index_get(db->idx, sk);
		if (data) {
			sv_clone.len = strlen(data);
			sv_clone.data = data;
			llru_set(db->lru, sk, &sv_clone);
		}
	}

	return data;
}

int db_exists(struct nessdb *db, struct slice *sk)
{
	char *val = index_get(db->idx, sk);
	if(val) {
		free(val);
		return 1;
	}

	return 0;
}

void db_remove(struct nessdb *db, struct slice *sk)
{
	llru_remove(db->lru, sk);
	index_add(db->idx, sk, NULL);
}

void db_info(struct nessdb *db, char *infos)
{
	/*TODO*/
}

void db_close(struct nessdb *db)
{
	index_free(db->idx);
	llru_free(db->lru);
	free(db);
}
