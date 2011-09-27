#ifndef _LLRU_H
#define _LLRU_H
#include "level.h"

void	llru_init(size_t buffer_size);
void	llru_set(const char *k,void *v,int k_len,int v_len);
void*	llru_get(const char *k);
void	llru_remove(const char *k);
void	llru_free();

#endif
