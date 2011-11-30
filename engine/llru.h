#ifndef _LLRU_H
#define _LLRU_H

#include <stdint.h>
#include "level.h"

struct llru{
	size_t nl_count;
	size_t ol_count;
	size_t buffer;
	uint64_t nl_allowsize;
	uint64_t nl_used;
	uint64_t ol_allowsize;
	uint64_t ol_used;

	struct ht *ht;
	struct level *level_old;
	struct level *level_new;
};


struct llru *llru_new(size_t buffer_size);
void llru_set(struct llru *llru, void *k, uint64_t v, size_t size);
uint64_t llru_get(struct llru *llru, void *k);
void llru_remove(struct llru *llru, void *k);
void llru_info(struct llru *llru);
void llru_free(struct llru *llru);

#endif
