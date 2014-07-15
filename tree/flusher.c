/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "cache.h"
#include "tree.h"
#include "node.h"
#include "flusher.h"

void _flush_buffer_to_child(struct tree *t, struct node *child, struct nmb *buf)
{
	struct nmb_iter iter;

	nmb_iter_init(&iter, buf);
	nmb_iter_seektofirst(&iter);
	while (nmb_iter_valid(&iter)) {
		/* TODO(BohuTANG): check msn */
		struct bt_cmd cmd = {
			.msn = iter.msn,
			.type = iter.type,
			.key = &iter.key,
			.val = &iter.val,
			.xidpair = iter.xidpair
		};
		node_put_cmd(t, child, &cmd);
		nmb_iter_next(&iter);
	}
}

void _flush_some_child(struct tree *t, struct node *parent);

/*
 * PROCESS:
 *	- check child reactivity
 *	- if FISSIBLE: split child
 *	- if FLUSHBLE: flush buffer from child
 * ENTER:
 *	- parent is already locked
 *	- child is already locked
 * EXIT:
 *	- parent is unlocked
 *	- no nodes are locked
 */
void _child_maybe_reactivity(struct tree *t, struct node *parent, struct node *child)
{
	enum reactivity re = get_reactivity(t, child);

	switch (re) {
	case STABLE:
		cache_unpin(t->cf, child);
		cache_unpin(t->cf, parent);
		break;
	case FISSIBLE:
		node_split_child(t, parent, child);
		cache_unpin(t->cf, child);
		cache_unpin(t->cf, parent);
		break;
	case FLUSHBLE:
		cache_unpin(t->cf, parent);
		_flush_some_child(t, child);
		break;
	}
}

/*
 * PROCESS:
 *	- pick a heaviest child of parent
 *	- flush from parent to child
 *	- maybe split/flush child recursively
 * ENTER:
 *	- parent is already locked
 * EXIT:
 *	- parent is unlocked
 *	- no nodes are locked
 */
void _flush_some_child(struct tree *t, struct node *parent)
{
	enum reactivity re;

	int childnum;
	struct node *child;
	struct partition *part;
	struct nmb *buffer;

	childnum = node_find_heaviest_idx(parent);
	nassert(childnum < parent->n_children);
	part = &parent->parts[childnum];
	buffer = part->ptr.u.nonleaf->buffer;
	if (cache_get_and_pin(t->cf, part->child_nid, &child, L_WRITE) != NESS_OK) {
		__ERROR("cache get node error, nid [%" PRIu64 "]", part->child_nid);
		return;
	}

	re = get_reactivity(t, child);
	if (re == STABLE) {
		node_set_dirty(parent);
		part->ptr.u.nonleaf->buffer = nmb_new();
		_flush_buffer_to_child(t, child, buffer);
		nmb_free(buffer);
	}
	_child_maybe_reactivity(t, parent, child);
}

/*
 * EFFECT:
 *	- do flush in a background thread
 * PROCESS:
 *	- if buf is NULL, we will do _flush_some_child
 *	- if buf is NOT NULL, we will do _flush_buffer_to_child
 * ENTER:
 *	- fe->node is already locked
 * EXIT:
 *	- nodes are unlocked
 */
static void _flush_node_func(void *fe)
{
	enum reactivity re;
	struct flusher_extra *extra = (struct flusher_extra*)fe;
	struct tree *t = extra->tree;
	struct node *n = extra->node;
	struct nmb *buf = extra->buffer;

	node_set_dirty(n);
	if (buf) {
		_flush_buffer_to_child(t, n, buf);
		nmb_free(buf);

		/* check the child node */
		re = get_reactivity(t, n);
		if (re == FLUSHBLE)
			_flush_some_child(t, n);
		else
			cache_unpin(t->cf, n);
	} else {
		/* we want flush some buffer from n */
		_flush_some_child(t, n);
	}

	xfree(extra);
}

/*
 * add work to background thread (non-block)
 */
static void _place_node_and_buffer_on_background(struct tree *t, struct node *node, struct nmb *buffer)
{
	struct flusher_extra *extra = xmalloc(sizeof(*extra));

	extra->tree = t;
	extra->node = node;
	extra->buffer = buffer;

	kibbutz_enq(t->cf->cache->c_kibbutz, _flush_node_func, extra);
}

/*
 * EFFECT:
 *	- flush in background thread
 * ENTER:
 *	- parent is already locked
 * EXIT:
 *	- nodes are all unlocked
 */
void tree_flush_node_on_background(struct tree *t, struct node *parent)
{
	int childnum;
	enum reactivity re;
	struct node *child;
	struct partition *part;

	nassert(parent->height > 0);
	childnum = node_find_heaviest_idx(parent);
	part = &parent->parts[childnum];

	/* pin the child */
	if (cache_get_and_pin(t->cf, part->child_nid, &child, L_WRITE) != NESS_OK) {
		__ERROR("cache get node error, nid [%" PRIu64 "]", part->child_nid);
		return;
	}

	re = get_reactivity(t, child);
	if (re == STABLE) {
		/* detach buffer from parent */
		struct nmb *buf = part->ptr.u.nonleaf->buffer;
		node_set_dirty(parent);
		part->ptr.u.nonleaf->buffer = nmb_new();

		/* flush it in background thread */
		_place_node_and_buffer_on_background(t, child, buf);
		cache_unpin(t->cf, parent);
	} else {
		/* the child is reactive, we deal it in main thread */
		_child_maybe_reactivity(t, parent, child);
	}
}
