/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MB_H_
#define nessDB_MB_H_

#include "internal.h"
#include "pma.h"

struct mb_iter {
	int valid;

	void *base;
	int chain_idx;
	int array_idx;
	struct pma *pma;
};

void mb_iter_init(struct mb_iter *, struct pma *);
int mb_iterate(struct mb_iter *);
int mb_iterate_on_range(struct mb_iter *,
                        struct pma_coord *,
                        struct pma_coord *);

#endif /* nessDB_MB_H_ */
