/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _SST_H
#define _SST_H

#include <stdint.h>
#include <pthread.h>
#include "buffer.h"
#include "skiplist.h"
#include "meta.h"
#include "bloom.h"
#include "util.h"

struct mutexer{
	volatile int lsn;
	pthread_mutex_t *mutex;
};

struct sst_block{
	char key[NESSDB_MAX_KEY_SIZE];
	uint64_t offset;
}__attribute__((packed));

struct sst{
	char basedir[FILE_PATH_SIZE];
	volatile int lsn;
	struct meta *meta;
	struct bloom *bloom;
	struct mutexer mutexer;
};

struct sst *sst_new(const char *basedir);
void sst_merge(struct sst *sst, struct skiplist *list, int fromlog);
uint64_t sst_getoff(struct sst *sst, struct slice *sk);
void sst_free(struct sst *sst);

#endif
