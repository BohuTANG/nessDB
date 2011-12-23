/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _SST_H
#define _SST_H

#include <stdint.h>
#include "skiplist.h"
#include "meta.h"
#include "util.h"

struct sst_block{
	char key[SKIP_KSIZE];
	uint64_t offset;
}__attribute__((packed));

struct sst{
	char basedir[SST_FLEN];
	char name[SST_NSIZE];
	uint32_t lsn;
	struct meta *meta;
};

struct sst *sst_new(const char *basedir);
void sst_merge(struct sst *sst, struct skiplist *list);
uint64_t sst_getoff(struct sst *sst, struct slice *sk);
void sst_free(struct sst *sst);

#endif
