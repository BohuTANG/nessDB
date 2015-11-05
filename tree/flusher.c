/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "t.h"

void _flush_buffer_to_child(struct tree *t, struct node *child, struct nmb *buf)
{
	struct mb_iter iter;

	mb_iter_init(&iter, buf->pma);
	while (mb_iter_next(&iter)) {
		/* TODO(BohuTANG): check msn */
		struct nmb_values nvalues;

		nmb_get_values(&iter, &nvalues);

		struct bt_cmd cmd = {
			.msn = nvalues.msn,
			.type = nvalues.type,
			.key = &nvalues.key,
			.val = &nvalues.val,
			.xidpair = nvalues.xidpair
		};
		node_put_cmd(t, child, &cmd);
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
		cache_unpin(t->cf, child->cpair, make_cpair_attr(child));
		cache_unpin(t->cf, parent->cpair, make_cpair_attr(parent));
		break;
	case FISSIBLE:
		node_split_child(t, parent, child);
		cache_unpin(t->cf, child->cpair, make_cpair_attr(child));
		cache_unpin(t->cf, parent->cpair, make_cpair_attr(parent));
		break;
	case FLUSHBLE:
		cache_unpin(t->cf, parent->cpair, make_cpair_attr(parent));
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
	int childnum;
	enum reactivity re;
	struct node *child;
	struct partition *part;
	struct nmb *buffer;
	struct timespec t1, t2;

	childnum = node_find_heaviest_idx(parent);
	nassert(childnum < parent->n_children);
	part = &parent->parts[childnum];
	buffer = part->ptr.u.nonleaf->buffer;
	if (cache_get_and_pin(t->cf, part->child_nid, (void**)&child, L_WRITE) != NESS_OK) {
		__ERROR("cache get node error, nid [%" PRIu64 "]", part->child_nid);
		return;
	}

	ngettime(&t1);
	re = get_reactivity(t, child);
	if (re == STABLE) {
		node_set_dirty(parent);
		part->ptr.u.nonleaf->buffer = nmb_new(t->e);
		_flush_buffer_to_child(t, child, buffer);
		nmb_free(buffer);
	}
	ngettime(&t2);
	status_add(&t->e->status->tree_flush_child_costs, time_diff_ms(t1, t2));
	status_increment(&t->e->status->tree_flush_child_nums);

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
			cache_unpin(t->cf, n->cpair, make_cpair_attr(n));
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
	if (cache_get_and_pin(t->cf, part->child_nid, (void**)&child, L_WRITE) != NESS_OK) {
		__ERROR("cache get node error, nid [%" PRIu64 "]", part->child_nid);
		return;
	}

	re = get_reactivity(t, child);
	if (re == STABLE) {
		/* detach buffer from parent */
		struct nmb *buf = part->ptr.u.nonleaf->buffer;
		node_set_dirty(parent);
		part->ptr.u.nonleaf->buffer = nmb_new(t->e);

		/* flush it in background thread */
		_place_node_and_buffer_on_background(t, child, buf);
		cache_unpin(t->cf, parent->cpair, make_cpair_attr(parent));
	} else {
		/* the child is reactive, we deal it in main thread */
		_child_maybe_reactivity(t, parent, child);
	}
}
