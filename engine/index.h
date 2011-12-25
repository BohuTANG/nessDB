/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _INDEX_H
#define _INDEX_H

#include "util.h"

struct m_list {
	int stable;
	struct skiplist *list;
	struct m_list *pre;
	struct m_list *nxt;
};

struct index{
	int lsn;
	int db_rfd;
	int meta_lsn;
	int max_mtbl;
	int max_mtbl_size;
	volatile int queue;

	char basedir[INDEX_NSIZE];
	char name[INDEX_NSIZE];
	struct log *log;
	struct sst *sst;
	struct m_list *head;
	struct m_list *last;
};

struct index *index_new(const char *basedir, const char *name, int max_mtbl_size);
int index_add(struct index *idx, struct slice *sk, struct slice *sv);
void index_flush(struct index *idx);
char *index_get(struct index *idx, struct slice *sk);
void index_free(struct index *idx);

#endif
