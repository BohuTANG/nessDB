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

struct llru {
	struct ht ht;
	struct level level_old;
	struct level level_new;
	int buffer;
};
typedef struct llru llru_t;

void	llru_init(llru_t *self, size_t buffer_size);
void	llru_set(llru_t *self, const char *k, void *v, size_t k_len, size_t v_len);
void*	llru_get(llru_t *self, const char *k);
void	llru_remove(llru_t *self, const char *k);
void	llru_info(llru_t *self, struct llru_info *llru_info);
void	llru_free(llru_t *self);

#endif
