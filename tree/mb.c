/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "mb.h"

/*
 * EFFECTS:
 *	- init iterator
 */
void mb_iter_init(struct mb_iter *iter, struct pma *pma)
{
	iter->valid = 0;
	iter->pma = pma;

	iter->chain_idx = 0;
	iter->array_idx = 0;
	iter->base = NULL;
}

/*
 * EFFECTS:
 *	- iterate the whole pma
 * RETURNS:
 *	- 0 means invalid
 *	- 1 means valid
 */
int mb_iterate(struct mb_iter *iter)
{
	struct pma *pma = iter->pma;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	if ((chain_idx < pma->used)
	    && (array_idx < pma->chain[chain_idx]->used)) {
		iter->base = pma->chain[chain_idx]->elems[array_idx];
		if (array_idx == (pma->chain[chain_idx]->used - 1)) {
			iter->chain_idx++;
			iter->array_idx = 0;
		} else {
			iter->array_idx++;
		}

		return 1;
	} else {
		return 0;
	}
}

/*
 * EFFECTS:
 *	- iterate the pma with range[left, right)
 * RETURNS:
 *	- 0 means invalid
 *	- 1 means valid
 */
int mb_iterate_on_range(struct mb_iter *iter,
                        struct pma_coord *left,
                        struct pma_coord *right)
{
	int cur_idx = iter->chain_idx + iter->array_idx;
	int min_idx = left->chain_idx + left->array_idx;
	int max_idx = right->chain_idx + right->array_idx;

	if (cur_idx >= min_idx && cur_idx < max_idx)
		return mb_iterate(iter);
	else
		return 0;
}
