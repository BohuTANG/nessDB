#ifndef __nessDB_COMPACT_H
#define __nessDB_COMPACT_H

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
