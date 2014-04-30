/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "cache.h"
#include "leaf.h"
#include "msgbuf.h"
#include "compare-func.h"
#include "cursor.h"

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
	int cmp;
	struct msg *pivot;

	switch (so->direction) {
	case SEARCH_FORWARD:
		pivot = &node->u.n.pivots[childnum];
		cmp = so->pivotbound_compare_func(so, pivot);

		while (childnum < (children - 1) && cmp >= 0)
			childnum++;
		break;
	case SEARCH_BACKWARD:
		pivot = &node->u.n.pivots[childnum - 1];
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
	if (p >= 0 && p < (int)(n->u.n.n_children - 1)) {
		if (so->pivot_bound)
			msgfree(so->pivot_bound);
		so->pivot_bound = msgdup(&n->u.n.pivots[p]);
	}
}

void ancestors_append(struct cursor *cur, struct msgbuf *bsm)
{
	struct ancestors *next_ance;

	next_ance = xcalloc(1, sizeof(*next_ance));
	next_ance->v = bsm;
	next_ance->next = cur->ances;
	cur->ances = next_ance;
	cur->ances_size++;
}

/*
 * get the innermost value for the same key(k1):
 *	+------------+
 *	|k1|msn1|val1|
 *	+------------+
 *	|k1|msn2|val2|
 *	+------------+
 *	|k1|msn3|val3|
 *	+------------+
 *
 *  val3 will get.
 */
int _get_innermost(struct cursor *cur, struct msgbuf_iter *iter)
{
	int ret = 0;

	while (msgbuf_iter_valid(iter)) {
		ret = 1;
		cur->valid = 1;
		cur->key = iter->key;
		cur->val = iter->val;
		ret = CURSOR_CONTINUE;

		if (iter->multi)
			msgbuf_iter_next(iter);
		else
			break;
	}

	return ret;
}

/*
 * get the visible value
 */
int _get_visible(struct cursor *cur, struct msgbuf_iter *iter)
{
	int ret = 0;

	while (msgbuf_iter_valid(iter)) {
		/* TODO: (BohuTANG) visibility checking */
		int visi = 1;

		if (iter->multi && visi) {
			ret = 1;
			cur->valid = 1;
			cur->key = iter->key;
			cur->val = iter->val;
			ret = CURSOR_CONTINUE;
			msgbuf_iter_next(iter);
		} else {
			break;
		}
	}

	return ret;
}

int _search_leaf(struct cursor *cur, struct search *so, struct node *leaf)
{
	int ret = CURSOR_EOF;

	/* first to apply all msgs to leaf bsm */
	leaf_apply_ancestors(leaf, cur->ances);

	switch (so->direction) {
	case SEARCH_FORWARD: {
			struct msgbuf_iter iter;

			iter.key = cur->key;
			msgbuf_iter_init(&iter, leaf->u.l.buffer);
C1:
			msgbuf_iter_next_diff(&iter);
			if (msgbuf_iter_valid(&iter)) {
				if (cur->txn) {
					/*
					 * if we got none from a key array
					 * restart the search from lable C1.
					 * if cur->key is k1, cur->txn is 3, and the arrays are:
					 * +-------------------+
					 * |k1|msn:1|txn:1|val1| --> step1
					 * +-------------------+
					 * |k2|msn:3|txn:8|val1| --> step2
					 * +-------------------+
					 * |k3|msn:5|txn:2|val1| --> step3
					 * +-------------------+
					 * now, the status are:
					 *	cur->key = k1
					 *	iter->key = k2
					 * since txn:8 is larger than 3, so we should skip the value in step2
					 * restart from C1 and goto step 3
					 */
					if (!_get_visible(cur, &iter)) {
						goto C1;
					} else {
						cur->valid = 1;
						cur->key = iter.key;
						cur->val = iter.val;
						cur->msgtype = iter.type;
						ret = CURSOR_CONTINUE;
					}
				} else {
					_get_innermost(cur, &iter);
				}
			} else {
				cur->valid = 0;
				ret = CURSOR_EOF;
			}
			break;
		}
	case SEARCH_BACKWARD: {
			struct msgbuf_iter iter;

			iter.key = cur->key;
			msgbuf_iter_init(&iter, leaf->u.l.buffer);
C2:
			msgbuf_iter_prev(&iter);
			if (msgbuf_iter_valid(&iter)) {
				if (cur->txn) {
					/*
					 * if we got none from a key array
					 * restart the search from lable C2.
					 */
					if (!_get_visible(cur, &iter)) {
						goto C2;
					} else {
						cur->valid = 1;
						cur->key = iter.key;
						cur->val = iter.val;
						cur->msgtype = iter.type;
						ret = CURSOR_CONTINUE;
					}
				} else {
					_get_innermost(cur, &iter);
				}
			} else {
				cur->valid = 0;
				ret = CURSOR_EOF;
			}
			break;
		}
	default:
		__PANIC("unsupport direction %u", so->direction);
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
	int child_to_search;
	NID child_nid;
	struct node *child;

	nassert(n->height > 0);
	ancestors_append(cur, n->u.n.parts[childnum].buffer);

	child_nid = n->u.n.parts[childnum].child_nid;
	if (cache_get_and_pin(cur->tree->cf, child_nid, &child, L_READ) < 0) {
		__ERROR("cache get node error, nid [%" PRIu64 "]", child_nid);

		return NESS_ERR;
	}

	child_to_search = _search_in_which_child(so, child);
	ret = _search_node(cur, so, child, child_to_search);

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
	int children;

	if (n->height > 0) {
		r = _search_child(cur, so, n, child_to_search);

		if (r == CURSOR_EOF) {
			_save_pivot_bound(so, n, child_to_search);
			switch (so->direction) {
			case SEARCH_FORWARD:
				children = (int)(n->u.n.n_children - 1);
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

try_again:
	root_nid = t->hdr->root_nid;
	if (cache_get_and_pin(t->cf, root_nid, &root, L_READ) < 0) {
		__ERROR("cache get root node error, nid [%" PRIu64 "]",
		        root_nid);

		return;
	}

	child_to_search = _search_in_which_child(so, root);
	r = _search_node(cur, so, root, child_to_search);

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
	default:
		break;
	}
}

void _tree_search_finish(struct search * so)
{
	msgfree(so->pivot_bound);
}

struct cursor *cursor_new(struct tree * t) {
	struct cursor *cur;

	cur = xcalloc(1, sizeof(*cur));
	cur->tree = t;

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
                       struct msg *key)
{
	memset(so, 0, sizeof(*so));
	so->direction_compare_func = dcmp;
	so->pivotbound_compare_func = pcmp;
	so->key_compare_func = msg_key_compare;
	so->direction = direction;
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
	                  &cur->key);

	_tree_search(cur, &search);
	_tree_search_finish(&search);
}
