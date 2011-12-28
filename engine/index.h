/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _INDEX_H
#define _INDEX_H

#include <pthread.h>
#include "skiplist.h"
#include "util.h"

struct idx_park{
	struct skiplist *list;
	int lsn;
};

struct index{
	int lsn;
	int db_rfd;
	int meta_lsn;
	int max_mtbl;
	int max_mtbl_size;

	char basedir[INDEX_NSIZE];
	char name[INDEX_NSIZE];
	struct log *log;
	struct sst *sst;
	struct skiplist *list;
	struct idx_park *park;
	pthread_mutex_t merge_mutex;
};

struct index *index_new(const char *basedir, const char *name, int max_mtbl_size, int tolog);
int index_add(struct index *idx, struct slice *sk, struct slice *sv);
char *index_get(struct index *idx, struct slice *sk);
void index_free(struct index *idx);

#endif
