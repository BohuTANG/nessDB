/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef __nessDB_SKIPLIST_H
#define __nessDB_SKIPLIST_H

#include "db.h"

#define MAXLEVEL (15)

struct skipnode{
	struct cola_item itm;
	struct skipnode *forward[1]; 
};

struct skiplist{
	int count;
	int size;
	int level; 

	char pool_embedded[1024];
	struct  skipnode *hdr;                 
	struct pool *pool;
};

struct skiplist *skiplist_new(size_t size);
int skiplist_insert(struct skiplist *list, struct cola_item *itm);
struct skipnode *skiplist_lookup(struct skiplist *list, char *data);
int skiplist_notfull(struct skiplist *list);
void skiplist_free(struct skiplist *list);

#endif
