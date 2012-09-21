/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _COMPACT_H
#define _COMPACT_H

#include <stdint.h>

struct cpt_node {
	uint64_t offset;
	struct cpt_node *nxt;
};

struct compact {
	int count;
	struct cpt_node **nodes;
};

struct compact *cpt_new();
int cpt_add(struct compact *cpt, int v_len, uint64_t offset);
uint64_t cpt_get(struct compact *cpt, int v_len);
void cpt_free(struct compact *cpt);

#endif
