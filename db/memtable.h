/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MEMTABLE_H_
#define nessDB_MEMTABLE_H_

#include "internal.h"
#include "options.h"
#include "logw.h"
#include "basement.h"

struct memtable {
	int ref;
	uint64_t msn;

	struct logw *lgw;
	struct basement *bsm;
	struct options *opts;
};

static inline void mtb_ref(struct memtable *mtb)
{
	mtb->ref++;
}

static inline void mtb_unref(struct memtable *mtb)
{
	mtb->ref--;
}

struct memtable *mtb_new(struct options *opts, uint64_t msn);

int mtb_put(struct memtable *mtb,
		struct msg *k,
		struct msg *v,
		msgtype_t type);

uint64_t mtb_memusage(struct memtable *mtb);

void mtb_free(struct memtable *mtb);

#endif /* nessDB_MEMTABLE_H_ */
