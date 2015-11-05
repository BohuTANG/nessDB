/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "t.h"

/**
 * @file tcursor.c
 * @brief
 *	scan the buffered-tree with cursor operation(FIRST, LAST, NEXT, PREV)
 */


/*
 * this is a 'follow the vine to get the melon' (root-to-leaf)
 * search->direction_compare_func is a helmsman to deep in which pivot we are
 * interested in.
 */
int _search_in_which_child(struct search *so, struct node *node)
{
	int lo, hi, mi, cmp;
	int children;
	int childnum = 0;

	/* i am leaf */
	if (node->height == 0)
		return 0;

	children = node->n_children;
	nassert(children >= 2);

	lo = 0;
	hi = children - 1;
	while (lo < hi) {
		/* mi integer overflow never happens */
		mi = (lo + hi) / 2;
		cmp = so->direction_compare_func(so, &node->pivots[mi]);
		if (((so->direction == SEARCH_FORWARD) && cmp) ||
		    ((so->direction == SEARCH_BACKWARD) && !cmp))
			hi = mi;
		else
			lo = mi + 1;
	}
	childnum = lo;

	/*
	 * detecting whether we should move to the prev/next pivot
	 * make a new root-to-leaf path
	 */
	struct msg *pivot;

	switch (so->direction) {
	case SEARCH_FORWARD:
		pivot = &node->pivots[childnum];
		cmp = so->pivotbound_compare_func(so, pivot);

		while (childnum < (children - 1) && cmp >= 0)
			childnum++;
		break;
	case SEARCH_BACKWARD:
		pivot = &node->pivots[childnum - 1];
		cmp = so->pivotbound_compare_func(so, pivot);

		while (childnum > 0 && cmp <= 0)
			childnum--;
		break;
	default:
		__PANIC("unsupport direction %u", so->direction);
	}

	return childnum;
}

/*
 * save the newly pivot bound to search->pivot_bound
 */
void _save_pivot_bound(struct search *so, struct node *n, int child_searched)
{
	nassert(n->height > 0);

	int p = (so->direction == SEARCH_FORWARD) ?
	        child_searched : child_searched - 1;
	if (p >= 0 && p < (int)(n->n_children - 1)) {
		if (so->pivot_bound)
			msgfree(so->pivot_bound);
		so->pivot_bound = msgdup(&n->pivots[p]);
	}
}

void ancestors_append(struct cursor *cur, struct nmb *nmb)
{
	struct ancestors *next_ance;

	next_ance = xcalloc(1, sizeof(*next_ance));
	next_ance->v = nmb;
	next_ance->next = cur->ances;
	cur->ances = next_ance;
	cur->ances_size++;
}

/*
 * if iso type is:
 *	a) uncommitted, the last one is visible
 *	b) serializable, the last one is visible
 *	c) committed: cur_id >= id, and id not in live roots list
 *	d) repeatable: cur_id >= id, and id not in live snapshot list
 *
 */
int _cursor_get_values_from_leafentry(struct cursor *cur, void *le)
{
	(void)cur;
	(void)le;
	nassert(0);

	return -1;
}

int _search_leaf(struct cursor *cur, struct search *so, struct node *leaf)
{
	int found;
	int ret = CURSOR_EOF;
	struct lmb *lmb;
	struct mb_iter iter;
	struct leafentry *le;
	struct pma_coord coord;

	/* 1) apply all msgs to leaf msgbuf */
	leaf_apply_ancestors(leaf, cur->ances);

	/* 2) init leaf iterator, TODO: which msgbuf to read */
	lmb = leaf->parts[0].ptr.u.leaf->buffer;
	mb_iter_init(&iter, lmb->pma);

	/* 3) do search */
	switch (so->gap) {
	case GAP_ZERO:
		found = lmb_find_zero(lmb, &cur->key, &le, &coord);
		if (found) {
			mb_iter_reset(&iter, &coord);
			if (mb_iter_valid(&iter)) {
				_cursor_get_values_from_leafentry(cur, iter.base);
			}
		}
		ret = CURSOR_EOF;
		break;
	case GAP_POSI:
		found = lmb_find_plus(lmb, &cur->key, &le, &coord);
		if (found) {
			mb_iter_reset(&iter, &coord);
POSI_RETRY:
			if (mb_iter_valid(&iter)) {
				int got = _cursor_get_values_from_leafentry(cur, iter.base);
				if (!got) {
					mb_iter_next(&iter);
					goto POSI_RETRY;
				} else {
					ret = CURSOR_EOF;
				}
			} else {
				ret = CURSOR_CONTINUE;
			}
		}
		break;
	case GAP_NEGA:
		found = lmb_find_minus(lmb, &cur->key, &le, &coord);
		if (found) {
			mb_iter_reset(&iter, &coord);
NEGA_RETRY:
			if (mb_iter_valid(&iter)) {
				int got = _cursor_get_values_from_leafentry(cur, iter.base);
				if (!got) {
					mb_iter_prev(&iter);
					goto NEGA_RETRY;
				} else {
					ret = CURSOR_EOF;
				}
			} else {
				ret = CURSOR_CONTINUE;
			}
		}
		break;
	}

	return ret;
}

int _search_node(struct cursor *, struct search *, struct node *, int);

/* search in a node's child */
int _search_child(struct cursor *cur,
                  struct search *so,
                  struct node *n,
                  int childnum)
{
	int ret;
	NID child_nid;
	int child_to_search;
	struct node *child;

	nassert(n->height > 0);
	ancestors_append(cur, n->parts[childnum].ptr.u.nonleaf->buffer);

	child_nid = n->parts[childnum].child_nid;
	if (!cache_get_and_pin(cur->tree->cf, child_nid, (void**)&child, L_READ)) {
		__ERROR("cache get node error, nid [%" PRIu64 "]", child_nid);

		return NESS_ERR;
	}

	child_to_search = _search_in_which_child(so, child);
	ret = _search_node(cur, so, child, child_to_search);

	/* unpin */
	cache_unpin(cur->tree->cf, child->cpair, make_cpair_attr(child));

	return ret;
}

int _search_node(struct cursor *cur,
                 struct search *so,
                 struct node *n,
                 int child_to_search)
{
	int r;
	int children;

	if (n->height > 0) {
		r = _search_child(cur, so, n, child_to_search);

		if (r == CURSOR_EOF) {
			_save_pivot_bound(so, n, child_to_search);
			switch (so->direction) {
			case SEARCH_FORWARD:
				children = (int)(n->n_children - 1);
				if (child_to_search < children)
					r = CURSOR_TRY_AGAIN;
				break;
			case SEARCH_BACKWARD:
				if (child_to_search > 0)
					r = CURSOR_TRY_AGAIN;
				break;
			default:
				break;
			}
		}
	} else {
		r = _search_leaf(cur, so, n);
	}

	return r;
}

/*
 *			|key44, key88|
 *			/		\
 *		  |key10, key20|	|key90|
 *	       /	|	 \	      \
 * |msgbuf0|	   |msgbuf1| |msgbuf2|    |msgbuf3|
 *
 *		      (a tree with height 2)
 *
 * cursor search is very similar to depth-first-search algorithm.
 * for cursor_seektofirst operation, the root-to-leaf path is:
 * key44 -> key10 -> msgbuf0
 * and do the inner sliding along with the msgbuf.
 * if we get the end of one leaf, CURSOR_EOF will be returned to upper on,
 * and we also set search->pivot_bound = key10, for the next time,
 * the root-to-leaf path(restart with a jump) will be:
 * key44 -> key10 -> msgbuf1
 */
void _tree_search(struct cursor * cur, struct search * so)
{
	int r;
	NID root_nid;
	int child_to_search;
	struct tree *t;
	struct node *root;

	t = cur->tree;

TRY_AGAIN:
	root_nid = t->hdr->root_nid;
	if (!cache_get_and_pin(t->cf, root_nid, (void**)&root, L_READ)) {
		__ERROR("cache get root node error, nid [%" PRIu64 "]", root_nid);

		return;
	}

	child_to_search = _search_in_which_child(so, root);
	r = _search_node(cur, so, root, child_to_search);

	/* unpin */
	cache_unpin(t->cf, root->cpair, make_cpair_attr(root));

	switch (r) {
	case CURSOR_CONTINUE:	  /* got the end of leaf */
		goto TRY_AGAIN;
		break;
	case CURSOR_TRY_AGAIN:	  /* got the end of node */
		goto TRY_AGAIN;
		break;
	case CURSOR_EOF:
		break;
	default:
		break;
	}
}

void _tree_search_finish(struct search * so)
{
	msgfree(so->pivot_bound);
}

struct cursor *cursor_new(struct tree * t, TXN *txn) {
	struct cursor *cur;

	cur = xcalloc(1, sizeof(*cur));
	cur->tree = t;
	cur->txn = txn;

	return cur;
}

void cursor_free(struct cursor * cur)
{
	struct ancestors *next;
	struct ancestors *ances = cur->ances;

	while (ances) {
		next = ances->next;
		xfree(ances);
		ances = next;
	}

	cur->ances = NULL;
	cur->ances_size = 0;

	xfree(cur);
}

void _tree_search_init(struct search *so,
                       search_direction_compare_func dcmp,
                       search_pivotbound_compare_func pcmp,
                       direction_t direction,
                       gap_t gap,
                       struct msg *key)
{
	memset(so, 0, sizeof(*so));
	so->direction_compare_func = dcmp;
	so->pivotbound_compare_func = pcmp;
	so->key_compare_func = msg_key_compare;
	so->direction = direction;
	so->gap = gap;
	if (key && key->data)
		so->key = key;
}

int search_pivotbound_compare(struct search *so, struct msg *m)
{
	return msg_key_compare(so->pivot_bound, m);
}

/*
 * tree cursor first
 */
int tree_cursor_compare_first(struct search *so __attribute__((__unused__)),
                              struct msg *b __attribute__((__unused__)))
{
	return -1;
}

int tree_cursor_valid(struct cursor *cur)
{
	return cur->valid == 1;
}

void tree_cursor_first(struct cursor *cur)
{
	struct search search;

	_tree_search_init(&search,
	                  &tree_cursor_compare_first,
	                  &search_pivotbound_compare,
	                  SEARCH_FORWARD,
	                  GAP_POSI,
	                  NULL);
	_tree_search(cur, &search);
	_tree_search_finish(&search);
}

/*
 * tree cursor last
 */
int tree_cursor_compare_last(struct search *so __attribute__((__unused__)),
                             struct msg *b __attribute__((__unused__)))
{
	return 1;
}

void tree_cursor_last(struct cursor *cur)
{
	struct search search;

	_tree_search_init(&search,
	                  &tree_cursor_compare_last,
	                  &search_pivotbound_compare,
	                  SEARCH_BACKWARD,
	                  GAP_NEGA,
	                  NULL);
	_tree_search(cur, &search);
	_tree_search_finish(&search);
}

/*
 * tree cursor next
 */
int tree_cursor_compare_next(struct search *so, struct msg *b)
{
	return (msg_key_compare(so->key, b) < 0);
}

void tree_cursor_next(struct cursor *cur)
{
	struct search search;

	_tree_search_init(&search,
	                  &tree_cursor_compare_next,
	                  &search_pivotbound_compare,
	                  SEARCH_FORWARD,
	                  GAP_POSI,
	                  &cur->key);
	_tree_search(cur, &search);
	_tree_search_finish(&search);
}

/*
 * tree cursor prev
 */
int tree_cursor_compare_prev(struct search *so, struct msg *b)
{
	return (msg_key_compare(so->key, b) > 0);
}

void tree_cursor_prev(struct cursor *cur)
{
	struct search search;

	_tree_search_init(&search,
	                  &tree_cursor_compare_prev,
	                  &search_pivotbound_compare,
	                  SEARCH_BACKWARD,
	                  GAP_NEGA,
	                  &cur->key);
	_tree_search(cur, &search);
	_tree_search_finish(&search);
}

/*
 * tree cursor current
 */
int tree_cursor_compare_current(struct search *so, struct msg *b)
{
	return (msg_key_compare(so->key, b) <= 0);
}

void tree_cursor_current(struct cursor *cur)
{
	struct search search;

	_tree_search_init(&search,
	                  &tree_cursor_compare_current,
	                  &search_pivotbound_compare,
	                  SEARCH_FORWARD,
	                  GAP_ZERO,
	                  &cur->key);
	_tree_search(cur, &search);
	_tree_search_finish(&search);
}
