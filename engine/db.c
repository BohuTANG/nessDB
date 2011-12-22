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

#include "db.h"

#define DB "ness"
#define LIST_SIZE	(2<<21)


struct nessdb *db_open(size_t bufferpool_size, const char *basedir)
{
	struct nessdb *db;
	 	
	db = malloc(sizeof(struct nessdb));
	db->idx = index_new(basedir, DB, LIST_SIZE);

	return db;
}


int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	return index_add(db->idx, sk, sv);
}

void  *db_get(struct nessdb *db, struct slice *sk)
{
	return index_get(db->idx, sk);
}

dbLine *db_get_all(struct nessdb *db, int *size)
{
  return index_get_all(db->idx, size);
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
	index_remove(db->idx, sk);
}


void db_info(struct nessdb *db, char *infos)
{
	/*TODO*/
}

void db_flush(struct nessdb *db)
{
	index_flush(db->idx);
}

void db_close(struct nessdb *db)
{
	index_free(db->idx);
	free(db);
}

void db_drop(struct nessdb *db)
{
	index_drop_all(db->idx);
	free(db);
}
