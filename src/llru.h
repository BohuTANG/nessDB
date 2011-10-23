#ifndef _LLRU_H
#define _LLRU_H
#include "level.h"

struct llru_info{
	int nl_count;
	int ol_count;
	uint64_t nl_allowsize;
	uint64_t nl_used;
	uint64_t ol_allowsize;
	uint64_t ol_used;
};

void	llru_init(size_t buffer_size);
void	llru_set(const char *k,void *v,int k_len,int v_len);
void*	llru_get(const char *k);
void	llru_remove(const char *k);
void	llru_info(struct llru_info *llru_info);
void	llru_free();

#endif
