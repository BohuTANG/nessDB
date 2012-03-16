/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _META_H
#define _META_H

#include <stdint.h>
#include <pthread.h>

#include "skiplist.h"
#include "util.h"
#include "config.h"

#define META_NODE_SIZE sizeof(struct meta_node)

struct meta_node{
	char end[NESSDB_MAX_KEY_SIZE];
	char index_name[FILE_NAME_SIZE];
	uint32_t count;
	uint32_t lsn;
};

struct meta{
	uint32_t sn;
	uint32_t size;
	struct meta_node nodes[META_MAX_COUNT];
};

struct meta *meta_new();
struct meta_node *meta_get(struct meta *meta, char *key);
void meta_set(struct meta *meta, struct meta_node *node);
void meta_set_byname(struct meta *meta, struct meta_node *node);
void meta_free(struct meta *meta);

#endif
