/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _LLRU_H
#define _LLRU_H

#include <stdint.h>
#include "ht.h"
#include "util.h"
#include "level.h"

struct llru{
	size_t buffer;
	uint64_t nl_allowsize;
	uint64_t nl_used;
	uint64_t ol_allowsize;
	uint64_t ol_used;

	struct ht *ht;
	struct level level_old;
	struct level level_new;
};

struct llru *llru_new(size_t buffer_size);
void llru_set(struct llru *llru, struct slice *sk, struct slice *sv);
struct slice *llru_get(struct llru *llru, struct slice *sk);
void llru_remove(struct llru *llru, struct slice *sk);
void llru_info(struct llru *llru);
void llru_free(struct llru *llru);

#endif
