/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "tcursor.h"
#include "cache.h"
#include "compare-func.h"

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
	int lo;
	int hi;
	int mi;
	int c;
	int children;
	int childnum = 0;

	/* i am leaf */
	if (node->height == 0)
		return 0;

	children = node->u.n.n_children;
	nassert(children >= 2);

	lo = 0;
	hi = children - 1;
	while (lo < hi) {
		/* mi integer overflow never happens */
		mi = (lo + hi) / 2;
		c = so->direction_compare_func(so, &node->u.n.pivots[mi]);
		if (((so->direction == SEARCH_FORWARD) && c) ||
				((so->direction == SEARCH_BACKWARD) && !c))
				hi = mi;
		else
			lo = mi + 1;
	}
	childnum = lo;

	/*
	 * detecting whether we should move to the prev/next pivot
	 * make a new root-to-leaf path
	 */
	switch (so->direction) {
	case SEARCH_FORWARD:
		while (childnum < (children - 1) &&
				so->pivotbound_compare_func(so,
					&node->u.n.pivots[childnum]) >= 0) {
			childnum++;
		}
		break;
	case SEARCH_BACKWARD:
		while (childnum > 0 &&
				so->pivotbound_compare_func(so,
					&node->u.n.pivots[childnum - 1]) <= 0) {
			childnum--;
		}
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
	if (p >= 0 && p < (int)(n->u.n.n_children - 1)) {
		if (so->pivot_bound)
			msgfree(so->pivot_bound);
		so->pivot_bound = msgdup(&n->u.n.pivots[p]);
	}
}

int _findsmallest(struct basement **bsms,
		int bsms_size,
		struct cursor *cur,
		struct search *so)
{
	int i;
	int ret;
	int end;
	struct basement_iter *smallest = NULL;
	struct basement_iter iters[bsms_size];
	
	/* seek each level */
	for (i = (bsms_size - 1); i >= 0; i--) {
		struct basement *bsm;
		struct basement_iter *iter;
		
		bsm = bsms[i];
		iter = &iters[i];
		basement_iter_init(iter, bsm);
		if (so->key) {
			/*
			 * sought position is >= position(so->key)
			 * so we should ensure that all sought iters positioned after so->key
			 */
			basement_iter_seek(iter, so->key);
			if (so->slip != SLIP_ZERO) {
				if (basement_iter_valid(iter) &&
						(so->key_compare_func(&iter->key, so->key) == 0))
					basement_iter_next(iter);
			}
		} else {
			basement_iter_seektofirst(iter);
		}
	}

	for (i = (bsms_size - 1); i >= 0; i--) {
		struct basement_iter *iter = &iters[i];

		if (basement_iter_valid(iter)) {
			if (smallest == NULL) {
				smallest = iter;
			} else if (so->key_compare_func(&iter->key, &smallest->key) < 0) {
				smallest = iter;
			}
		}
	}

	end = (basement_iter_valid(&iters[0]) == 0);
	if (end) {
		if (smallest) {
			cur->valid = 1;
			cur->key = smallest->key;
			cur->val = smallest->val;
		} else {
			cur->valid = 0;
		}

		ret = CURSOR_EOF;
	} else {
		cur->valid = 1;
		cur->key = smallest->key;
		cur->val = smallest->val;
		ret = CURSOR_CONTINUE;
	}

	return ret;
}

int _findlargest(struct basement **bsms,
		int bsms_size,
		struct cursor *cur,
		struct search *so)
{
	int i;
	int ret;
	int end;
	struct basement_iter *largest = NULL;
	struct basement_iter iters[bsms_size];
	
	/* seek each level */
	for (i = (bsms_size - 1); i >= 0; i--) {
		struct basement *bsm;
		struct basement_iter *iter;
		
		bsm = bsms[i];
		iter = &iters[i];
		basement_iter_init(iter, bsm);
		if (so->key) {
			/*
			 * since basement_iter_seek is >=so->key,
			 * we should ensure that all sought iters positioned before so->key
			 */
			basement_iter_seek(iter, so->key);
			if (basement_iter_valid(iter))
				basement_iter_prev(iter);
			else
				basement_iter_seektolast(iter);
		} else {
			basement_iter_seektolast(iter);
		}
	}

	for (i = (bsms_size - 1); i >= 0; i--) {
		struct basement_iter *iter = &iters[i];

		if (basement_iter_valid(iter)) {
			if (largest == NULL) {
				largest = iter;
			} else if (so->key_compare_func(&iter->key, &largest->key) > 0) {
				largest = iter;
			}
		}
	}

	end = (basement_iter_valid(&iters[0]) == 0);
	if (end) {
		if (largest) {
			cur->valid = 1;
			cur->key = largest->key;
			cur->val = largest->val;
		} else
			cur->valid = 0;

		ret = CURSOR_EOF;
	} else {
		cur->valid = 1;
		cur->key = largest->key;
		cur->val = largest->val;
		ret = CURSOR_CONTINUE;
	}

	return ret;
}

void ancestors_append(struct cursor *cur, struct basement *bsm)
{
	struct ancestors *next_ance;

	next_ance = xcalloc(1, sizeof(*next_ance));
	next_ance->v = bsm;
	next_ance->next = cur->ances;
	cur->ances = next_ance;
	cur->ances_size++;
}

int _search_leaf(struct cursor *cur, struct search *so, struct node *n)
{
	int i = 0;
	int bsms_size;
	struct ancestors *ances;

	ancestors_append(cur, n->u.l.le->bsm);
	ances = cur->ances;
	bsms_size = cur->ances_size;

	struct basement *bsms[bsms_size];
	/*
	 * bsms[0 - 1] is memtable basement
	 * the last one is root's
	 */
	while (ances) {
		bsms[i++] = (struct basement*) ances->v;
		ances = ances->next;
	}

	switch (so->direction) {
	case SEARCH_FORWARD:
		return _findsmallest(bsms,
				bsms_size,
				cur,
				so);
		break;
	case SEARCH_BACKWARD:
		return _findlargest(bsms,
				bsms_size,
				cur,
				so);
		break;
	default:
		__PANIC("unsupport direction %u", so->direction);
	}
}

int _search_node(struct cursor *cur,
		struct search *so,
		struct node *n,
		int child_to_search);

/* search in a node's child */
int _search_child(struct cursor *cur,
		struct search *so,
		struct node *n,
		int childnum)
{
	int ret;
	int child_to_search;
	NID child_nid;
	struct node *child;

	nassert(n->height > 0);
	/* add basement to ances */
	ancestors_append(cur, n->u.n.parts[childnum].buffer);

	child_nid = n->u.n.parts[childnum].child_nid;
	if (cache_get_and_pin(cur->tree->cf, child_nid, &child, L_READ) < 0) {
		__ERROR("cache get node error, nid [%" PRIu64 "]",
				child_nid);

		return NESS_ERR;
	}

	child_to_search = _search_in_which_child(so, child);
	ret = _search_node(cur,
			so,
			child,
			child_to_search);

	/* unpin */
	cache_unpin_readonly(cur->tree->cf, child);

	return ret;
}

int _search_node(struct cursor *cur,
		struct search *so,
		struct node *n,
		int child_to_search)
{
	int r;

	if (n->height > 0) {
		r = _search_child(cur, so, n, child_to_search);

		if (r == CURSOR_EOF) {
			_save_pivot_bound(so, n, child_to_search);
			switch (so->direction) {
			case SEARCH_FORWARD:
				if (child_to_search < (int)(n->u.n.n_children - 1))
					r = CURSOR_TRY_AGAIN;
				break;
			case SEARCH_BACKWARD:
				if (child_to_search > 0)
					r = CURSOR_TRY_AGAIN;
				break;
			default:break;
			}
		}
	} else
		r = _search_leaf(cur, so, n);

	return r;
}

/*
 *			|key44, key88|
 *			/		\
 *		  |key10, key20|	|key90|
 *	       /	|	 \	      \
 * |basement0|	   |basement1| |basement2|    |basement3|
 *
 *		      (a tree with height 2)
 *
 * cursor search is very similar to depth-first-search algorithm.
 * for cursor_seektofirst operation, the root-to-leaf path is:
 * key44 -> key10 -> basement0
 * and do the inner sliding along with the basement.
 * if we get the end of one leaf, CURSOR_EOF will be returned to upper on,
 * and we also set search->pivot_bound = key10, for the next time,
 * the root-to-leaf path(restart with a jump) will be:
 * key44 -> key10 -> basement1
 */
void _tree_search(struct cursor *cur, struct search *so)
{
	int r;
	NID root_nid;
	int child_to_search;
	struct tree *t;
	struct node *root;

	t = cur->tree;

try_again:
	root_nid = t->hdr->root_nid;
	if (cache_get_and_pin(t->cf, root_nid, &root, L_READ) < 0) {
		__ERROR("cache get root node error, nid [%" PRIu64 "]",
				root_nid);

		return;
	}

	child_to_search = _search_in_which_child(so, root);
	r = _search_node(cur,
			so,
			root,
			child_to_search);

	/* unpin */
	cache_unpin_readonly(t->cf, root);

	switch (r) {
	case CURSOR_CONTINUE:
		break;
	case CURSOR_TRY_AGAIN:
		goto try_again;
		break;
	case CURSOR_EOF:
		break;
	default: break;
	}
}

void _tree_search_finish(struct search *so)
{
	msgfree(so->pivot_bound);
}

struct cursor *cursor_new(struct tree *t)
{
	struct cursor *cur;

	cur = xcalloc(1, sizeof(*cur));
	cur->tree = t;

	return cur;
}

void cursor_free(struct cursor *cur)
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
		slip_t slip,
		struct msg *key)
{
	memset(so, 0, sizeof(*so));
	so->direction_compare_func = dcmp;
	so->pivotbound_compare_func = pcmp;
	so->key_compare_func = msg_key_compare;
	so->direction = direction;
	so->slip = slip;
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
			SLIP_NEGA,
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
			SLIP_NEGA,
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
			SLIP_POSI,
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
			SLIP_NEGA,
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
			SLIP_ZERO,
			&cur->key);

	_tree_search(cur, &search);
	_tree_search_finish(&search);
}
