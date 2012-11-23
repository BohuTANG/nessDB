#ifndef __FC_H
#define __FC_H

#include "config.h"
#include "cola.h"

#define GAP (8)

struct fc {
	int sst_count;
	struct cola_item *fcs;
};

struct fc *fc_new(int sst_count);
void fc_build(struct fc *fc, struct cola_item *item, int count, int level);
int fc_search(struct fc *fc, const char *key, int level);
void fc_free(struct fc *fc);

#endif
