/*
 * nessDB storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _LEVEL_H
#define _LEVEL_H

#include <stdint.h>
#include "util.h"

struct level{
	size_t count;
	size_t allow_size;
	size_t used_size;

	struct level_node *first;
	struct level_node *last;
};

struct level_node{
	int hits;
	size_t size;
	struct slice sk;
	struct slice sv;
	struct level_node *nxt;
	struct level_node *pre;
};

struct level *level_creat();
void level_set_head(struct level *level, struct level_node *node);
void level_remove_link(struct level *, struct level_node *node);
void level_free_last(struct level *level);
void level_free_node(struct level *level, struct level_node *node);
void level_free(struct level *level);

#endif
