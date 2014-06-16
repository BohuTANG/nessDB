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

void ancestors_append(struct cursor *cur, struct fifo *fifo)
{
	struct ancestors *next_ance;

	next_ance = xcalloc(1, sizeof(*next_ance));
	next_ance->v = fifo;
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
int _get_visible_value(struct cursor *cur, struct msgbuf_iter *iter)
{
	TXNID id;
	TXNID curid = TXNID_NONE;

	int visi = 0;
	int islive;

	struct txnmgr *tmgr = NULL;
	struct txnid_snapshot *lives;

	if (cur->txn) {
		tmgr = cur->txn->logger->txnmgr;
		curid = cur->txn->root_parent_txnid;
	}
	while (msgbuf_internal_iter_reverse(iter)) {
		/* in non-transaction */
		if (!cur->txn) {
			visi = 1;
			goto GOT_VALUE;
		}

		/* in transaction */
		switch (cur->iso_type) {
		case TXN_ISO_SERIALIZABLE:
		case TXN_ISO_READ_UNCOMMITTED:
			visi = 1;
			goto GOT_VALUE;
		case TXN_ISO_READ_COMMITTED:
			id = iter->xidpair.parent_xid;
			lives = tmgr->live_root_txnids;
			islive = txnmgr_txn_islive(lives, id);
			if (!islive && (curid >= id)) {
				visi = 1;
				goto GOT_VALUE;
			}
			break;
		case TXN_ISO_REPEATABLE:
			id = iter->xidpair.parent_xid;
			lives = cur->txn->txnid_clone;
			islive = txnmgr_txn_islive(lives, id);
			if (!islive && (curid >= id)) {
				visi = 1;
				goto GOT_VALUE;
			}
			break;
		}
	}

GOT_VALUE:
	if (visi) {
		cur->valid = 1;
		cur->msgtype = iter->type;
		cur->key = iter->key;
		cur->val = iter->val;
	}

	return visi;
}

int _search_leaf(struct cursor *cur, struct search *so, struct node *leaf)
{
	int ret = CURSOR_EOF;
	struct msgbuf_iter iter;

	/* first to apply all msgs to leaf msgbuf */
	leaf_apply_ancestors(leaf, cur->ances);

	/* init iter */
	iter.key = cur->key;
	msgbuf_iter_init(&iter, leaf->u.l.buffer);

	switch (so->gap) {
	case GAP_ZERO:
		nassert(so->direction == SEARCH_FORWARD);
		msgbuf_iter_seek(&iter, so->key);
		if (msgbuf_iter_valid(&iter)) {
			if (_get_visible_value(cur, &iter) &&
			    iter.type != MSG_DELETE) {
				ret = CURSOR_CONTINUE;
				goto RET;
			}
		} else {
			cur->valid = 0;
			ret = CURSOR_EOF;
		}
		break;
	case GAP_POSI:
		nassert(so->direction == SEARCH_FORWARD);
		if (!so->key)
			msgbuf_iter_seektofirst(&iter);
		else
			msgbuf_iter_seek(&iter, so->key);
		while (msgbuf_iter_valid(&iter)) {
			msgbuf_iter_next(&iter);
			if (msgbuf_iter_valid(&iter)) {
				if (_get_visible_value(cur, &iter) &&
				    iter.type != MSG_DELETE) {
					ret = CURSOR_CONTINUE;
					goto RET;
				}
			} else {
				goto POSI_RET;
			}
		}
POSI_RET:
		cur->valid = 0;
		ret = CURSOR_EOF;
		break;
	case GAP_NEGA:
		nassert(so->direction == SEARCH_BACKWARD);
		if (!so->key)
			msgbuf_iter_seektolast(&iter);
		else
			msgbuf_iter_seek(&iter, so->key);
		while (msgbuf_iter_valid(&iter)) {
			msgbuf_iter_prev(&iter);
			if (msgbuf_iter_valid(&iter)) {
				if (_get_visible_value(cur, &iter) &&
				    iter.type != MSG_DELETE) {
					ret = CURSOR_CONTINUE;
					goto RET;
				}
			} else {
				goto NEGA_RET;
			}
		}
NEGA_RET:
		cur->valid = 0;
		ret = CURSOR_EOF;
		break;
	default:
		cur->valid = 0;
		ret = CURSOR_EOF;
		break;
	}

RET:
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
	cache_unpin(cur->tree->cf, child);

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
	cache_unpin(t->cf, root);

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
