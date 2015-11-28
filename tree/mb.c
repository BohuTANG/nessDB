/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"

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

void mb_iter_reset(struct mb_iter *iter, struct pma_coord *coord)
{
	iter->valid = 0;
	iter->chain_idx = coord->chain_idx;
	iter->array_idx = coord->array_idx;
	iter->base = NULL;
}

int mb_iter_valid(struct mb_iter *iter)
{
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	return ((chain_idx >= 0) && (chain_idx < iter->pma->used))
	       && ((array_idx >= 0) && (array_idx < iter->pma->chain[chain_idx]->used));
}


/*
 * EFFECTS:
 *	- iterate the whole pma with forward direction
 * RETURNS:
 *	- 0 means invalid
 *	- 1 means valid
 */
int mb_iter_next(struct mb_iter *iter)
{
	struct pma *pma = iter->pma;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	if (mb_iter_valid(iter)) {
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
 *	- iterate the whole pma with backward direction
 * RETURNS:
 *	- 0 means invalid
 *	- 1 means valid
 */
int mb_iter_prev(struct mb_iter *iter)
{
	struct pma *pma = iter->pma;
	int chain_idx = iter->chain_idx;
	int array_idx = iter->array_idx;

	if (mb_iter_valid(iter)) {
		iter->base = pma->chain[chain_idx]->elems[array_idx];
		if (array_idx == 0) {
			iter->chain_idx--;
			iter->array_idx = pma->chain[chain_idx]->used - 1;
		} else {
			iter->array_idx--;
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
int mb_iter_on_range(struct mb_iter *iter,
                     struct pma_coord *left,
                     struct pma_coord *right)
{
	int cur_idx = iter->chain_idx + iter->array_idx;
	int min_idx = left->chain_idx + left->array_idx;
	int max_idx = right->chain_idx + right->array_idx;

	if (cur_idx >= min_idx && cur_idx < max_idx)
		return mb_iter_next(iter);
	else
		return 0;
}
