/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MB_H_
#define nessDB_MB_H_

struct mb_iter {
	int valid;

	void *base;
	int chain_idx;
	int array_idx;
	struct pma *pma;
};

void mb_iter_init(struct mb_iter *, struct pma *);
void mb_iter_reset(struct mb_iter *, struct pma_coord *coord);
int mb_iter_valid(struct mb_iter *);
int mb_iter_next(struct mb_iter *);
int mb_iter_prev(struct mb_iter *);
int mb_iter_on_range(struct mb_iter *,
                     struct pma_coord *,
                     struct pma_coord *);

#endif /* nessDB_MB_H_ */
