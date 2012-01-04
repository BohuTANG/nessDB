 /*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
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

int db_get(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	int ret = 0;
	char *data;
	struct slice *sv_l;

	sv_l = llru_get(db->lru, sk);
	if (sv_l) {
		data = malloc(sv_l->len);
		memcpy(data, sv_l->data, sv_l->len);
		sv->len = sv_l->len;
		sv->data = data;
		ret = 1;
	} else {
		ret = index_get(db->idx, sk, sv);
		if (ret) {
			struct slice sv_clone;

			/* Clone new data for LLRU object */
			data = malloc(sv->len);
			memcpy(data, sv->data, sv->len);

			sv_clone.len = sv->len;
			sv_clone.data = data;
			llru_set(db->lru, sk, &sv_clone);
		}

	}

	return ret;
}

int db_exists(struct nessdb *db, struct slice *sk)
{
	struct slice sv;
	int ret = index_get(db->idx, sk, &sv);
	if(ret) {
		free(sv.data);
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
	llru_free(db->lru);
	index_free(db->idx);
	free(db);
}
