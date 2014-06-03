/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "cache.h"
#include "hdrse.h"
#include "se.h"
#include "file.h"
#include "leaf.h"
#include "txn.h"
#include "flusher.h"
#include "tree.h"

/*
 *
 * if the layout as follows(3 pivots, 4 partitions):
 *
 * 		+--------+--------+--------+
 * 		|   15   |   17   |   19   | +∞
 * 		+--------+--------+--------+\
 * 		  pidx0    pidx1    pidx2    pidx3
 *
 *
 * so if spk is 16, pidx = 1, after added(4 pivtos, 5 partitions):
 *
 * 		+--------+--------+--------+--------+
 * 		|   15   |  [16]  |   17   |   19   | +∞
 * 		+--------+--------+--------+--------+\
 * 		  pidx0   [pidx1]   pidx2    pidx3     pidx4
 *
 *
 * ENTRY:
 *	- parent is already locked(L_WRITE)
 *	- node a is already locked(L_WRITE)
 *	- node b is already locked(L_WRITE)
 */
void _add_pivot_to_parent(struct tree *t,
                          struct node *parent,
                          int child_num,
                          struct node *a,
                          struct node *b,
                          struct msg *spk)
{
	int i;
	int pidx;

	pidx = child_num;
	parent->u.n.pivots = xrealloc(parent->u.n.pivots,
	                              parent->u.n.n_children * PIVOT_SIZE);

	parent->u.n.parts = xrealloc(parent->u.n.parts,
	                             (parent->u.n.n_children + 1) * PART_SIZE);

	/* slide pivots */
	for (i = parent->u.n.n_children - 1; i > pidx; i--) {
		parent->u.n.pivots[i] = parent->u.n.pivots[i - 1];
	}

	/* slide parts */
	for (i = parent->u.n.n_children; i > pidx; i--) {
		parent->u.n.parts[i] = parent->u.n.parts[i - 1];
	}

	msgcpy(&parent->u.n.pivots[pidx], spk);
	parent->u.n.parts[pidx].child_nid = a->nid;
	parent->u.n.parts[pidx].buffer = fifo_new();

	parent->u.n.parts[pidx + 1].child_nid = b->nid;
	parent->u.n.n_children += 1;
	node_set_dirty(parent);

	status_increment(&t->status->tree_add_pivots_nums);
}

/*
 * split leaf to two leaves:a & b
 * leaf(root):
 * 	+-----------------------------------+
 * 	|  0  |  1  |  2  |  3  |  4  |  5  |
 * 	+-----------------------------------+
 *
 * split:
 *			   root
 * 			 +--------+
 * 			 |   2    |
 * 			 +--------+
 *	                /          \
 * 	+-----------------+	 +------------------+
 * 	|  0  |  1  |  2  |	 |  3  |  4  |  5   |
 * 	+-----------------+	 +------------------+
 * 	      nodea			nodeb
 *
 *
 * ENTRY:
 *	- leaf is already locked (L_WRITE)
 * EXITS:
 *	- a is locked
 *	- b is locked
 */
void _leaf_split(struct tree *t,
                 struct node *leaf,
                 struct node **a,
                 struct node **b,
                 struct msg **split_key)
{
	struct node *leafa;
	struct node *leafb;
	struct msgbuf *mb;
	struct msgbuf *mba;
	struct msgbuf *mbb;

	leafa = leaf;
	mb = leafa->u.l.buffer;
	msgbuf_split(mb, &mba, &mbb, split_key);

	leafa->u.l.buffer = mba ;

	/* new leafb */
	cache_create_node_and_pin(t->cf, 0, 0, &leafb);
	leafb->u.l.buffer = mbb;

	/* set dirty */
	node_set_dirty(leafa);
	node_set_dirty(leafb);

	*a = leafa;
	*b = leafb;

	status_increment(&t->status->tree_leaf_split_nums);
}

/*
 * 	+-----------------------------------------------+
 * 	| 	5 	| 	7 	| 	9 	|
 * 	+-----------------------------------------------+
 * 				|
 * 			+---------------+
 * 			|  60 | 61 | 62 |
 * 			+---------------+
 *
 * 	+---------------------------------------------------------------+
 * 	|  	5 	| 	60	 |	7 	| 	9 	|
 * 	+---------------------------------------------------------------+
 * 				|		|
 * 			    +--------+	    +---------+
 * 			    |   60   |	    | 61 | 62 |
 * 			    +--------+	    +---------+
 *
 *
 * ENTRY:
 *	- node is already locked(L_WRITE)
 * EXITS:
 *	- a is locked(L_WRITE)
 *	- b is locked(L_WRITE)
 */
void _nonleaf_split(struct tree *t,
                    struct node *node,
                    struct node **a,
                    struct node **b,
                    struct msg **split_key)
{
	int i;
	int pivots_old;
	int pivots_in_a;
	int pivots_in_b;
	struct node *nodea;
	struct node *nodeb;
	struct msg *spk;

	nodea = node;
	pivots_old = node->u.n.n_children - 1;
	nassert(pivots_old > 2);

	pivots_in_a = pivots_old / 2;
	pivots_in_b = pivots_old - pivots_in_a;

	/* node a */
	nodea->u.n.n_children = pivots_in_a + 1;

	/* node b */
	cache_create_node_and_pin(t->cf,
	                          1,			/* height */
	                          pivots_in_b + 1,	/* children */
	                          &nodeb);

	for (i = 0; i < (pivots_in_b); i++) {
		nodeb->u.n.pivots[i] = node->u.n.pivots[pivots_in_a + i];
	}

	for (i = 0; i < (pivots_in_b + 1); i++) {
		nodeb->u.n.parts[i] = node->u.n.parts[pivots_in_a + i];
	}

	/* the rightest partition of nodea */
	nodea->u.n.parts[pivots_in_a].buffer = fifo_new();

	/* split key */
	spk = msgdup(&node->u.n.pivots[pivots_in_a - 1]);

	node_set_dirty(nodea);
	node_set_dirty(nodeb);

	*a = nodea;
	*b = nodeb;
	*split_key = spk;

	status_increment(&t->status->tree_nonleaf_split_nums);
}

/*
 * EFFECT:
 *	- split the child
 *	- add a new pivot(split-key) to parent
 * ENTRY:
 *	- parent is already locked(L_WRITE)
 *	- child is already locked(L_WRITE)
 * EXITS:
 *	- parent is locked
 *	- child is locked
 */
void node_split_child(struct tree *t,
                      struct node *parent,
                      struct node *child)
{
	int child_num;
	struct node *a;
	struct node *b;
	struct msg *split_key;

	if (child->height == 0) {
		_leaf_split(t, child, &a, &b, &split_key);
	} else {
		_nonleaf_split(t, child, &a, &b, &split_key);
	}

	child_num = node_partition_idx(parent, split_key);

	/* add pivot to parent */
	_add_pivot_to_parent(t, parent, child_num, a, b, split_key);
	msgfree(split_key);
	cache_unpin(t->cf, b);

	status_increment(&t->status->tree_nonleaf_split_nums);
}

enum reactivity get_reactivity(struct tree *t, struct node *node)
{
	if (nessunlikely(node->height == 0)) {
		if ((node_size(node) >= t->opts->leaf_node_page_size && node_count(node) > 1) ||
		    node_count(node) >= t->opts->leaf_node_page_count)
			return FISSIBLE;
	} else {
		uint32_t children = node->u.n.n_children;

		if (children >= t->opts->inner_node_fanout)
			return FISSIBLE;

		if (((node_size(node) > t->opts->inner_node_page_size)) ||
		    node_count(node) >= t->opts->inner_node_page_count)
			return FLUSHBLE;
	}

	return STABLE;
}

/*
 * EFFECT:
 *	- put cmd to leaf
 * ENTRY:
 *	- leaf is already locked(L_WRITE)
 * EXITS:
 *	- leaf is locked
 */
void leaf_put_cmd(struct node *leaf, struct bt_cmd *cmd)
{
	rwlock_write_lock(&leaf->u.l.rwlock, &leaf->u.l.mtx);
	leaf_apply_msg(leaf, cmd);
	leaf->msn = cmd->msn > leaf->msn ? cmd->msn : leaf->msn;
	node_set_dirty(leaf);
	rwlock_write_unlock(&leaf->u.l.rwlock);
}

/*
 * EFFECT:
 *	- put cmd to nonleaf
 * ENTRY:
 *	- node is already locked (L_READ)
 * EXITS:
 *	- node is locked
 */
void nonleaf_put_cmd(struct node *node, struct bt_cmd *cmd)
{
	uint32_t pidx;
	struct partition *part;

	pidx = node_partition_idx(node, cmd->key);
	part = &node->u.n.parts[pidx];

	if (!part->buffer)
		__PANIC("partiton buffer is null, index %d", pidx);

	rwlock_write_lock(&part->rwlock, &part->mtx);
	fifo_append(part->buffer,
	            cmd-> msn,
	            cmd->type,
	            cmd->key,
	            cmd->val,
	            &cmd->xidpair);
	node->msn = cmd->msn > node->msn ? cmd->msn : node->msn;
	node_set_dirty(node);
	rwlock_write_unlock(&part->rwlock);
}

/*
 * EFFECT:
 *	- put cmd to node
 * ENTRY:
 *	- node is already locked(L_WRITE)
 * EXITS:
 *	- node is locked
 */
void node_put_cmd(struct tree *t, struct node *node, struct bt_cmd *cmd)
{
	if (nessunlikely(node->height == 0)) {
		leaf_put_cmd(node, cmd);
		status_increment(&t->status->tree_leaf_put_nums);
	} else {
		nonleaf_put_cmd(node, cmd);
		status_increment(&t->status->tree_nonleaf_put_nums);
	}
}

/*
 * EFFECT:
 *	- in order to keep the root NID is eternal
 * ENTRY:
 *	- old_root is already locked
 *	- new_root is already locked
 * EXITS:
 *	- old_root is already locked
 *	- new_root is already locked
 */
void _root_swap(struct node *new_root, struct node *old_root)
{
	MSN old_msn;
	MSN new_msn;
	NID old_nid;
	NID new_nid;

	struct cpair *old_cp;
	struct cpair *new_cp;

	/* swap msn */
	old_msn = old_root->msn;
	new_msn = new_root->msn;
	old_root->msn = new_msn;
	new_root->msn = old_msn;

	/* swap nid */
	old_nid = old_root->nid;
	new_nid = new_root->nid;
	old_root->nid = new_nid;
	new_root->nid = old_nid;

	/* swap cpair */
	old_cp = old_root->cpair;
	new_cp = new_root->cpair;
	old_root->cpair = new_cp;
	new_root->cpair = old_cp;
	old_cp->v = new_root;
	new_cp->v = old_root;

	/* swap root flag */
	old_root->isroot = 0;
	new_root->isroot = 1;
}

void _root_split(struct tree *t,
                 struct node *new_root,
                 struct node *old_root)
{
	struct node *a;
	struct node *b;
	struct msg *split_key;

	if (old_root->height == 0) {
		_leaf_split(t, old_root, &a, &b, &split_key);
	} else {
		_nonleaf_split(t, old_root, &a, &b, &split_key);
	}

	/* swap two roots */
	_root_swap(new_root, old_root);

	msgcpy(&new_root->u.n.pivots[0], split_key);
	new_root->u.n.parts[0].child_nid = a->nid;
	new_root->u.n.parts[0].buffer = fifo_new();

	new_root->u.n.parts[1].child_nid = b->nid;
	new_root->u.n.parts[1].buffer = fifo_new();
	msgfree(split_key);

	node_set_dirty(b);
	cache_unpin(t->cf, b);

	node_set_dirty(old_root);
	node_set_dirty(new_root);

	t->hdr->height++;
	status_increment(&t->status->tree_root_new_nums);
}

/*
 * EFFECT:
 *	- split the fissible root
 */
void _root_fissible(struct tree *t, struct node *root)
{
	struct node *new_root;
	uint32_t new_root_height = 1;
	uint32_t new_root_children = 2;

	cache_create_node_and_pin(t->cf, new_root_height, new_root_children, &new_root);
	_root_split(t, new_root, root);

	cache_unpin(t->cf, root);
	cache_unpin(t->cf, new_root);
}

/*
 * PROCESS:
 *	- put cmd to root
 *	- check root reactivity
 *	    -- FISSIBLE: split root
 *	    -- FLUSHABLE: flush root in background
 * ENTRY:
 *	- no nodes are locked
 * EXITS:
 *	- all nodes are unlocked
 */
int root_put_cmd(struct tree *t, struct bt_cmd *cmd)
{
	struct node *root;
	enum reactivity re;
	volatile int hasput = 0;
	enum lock_type locktype = L_READ;

CHANGE_LOCK_TYPE:
	if (!cache_get_and_pin(t->cf, t->hdr->root_nid, &root, locktype))
		return NESS_ERR;

	if (!hasput) {
		node_put_cmd(t, root, cmd);
		hasput = 1;
	}

	re = get_reactivity(t, root);
	switch (re) {
	case STABLE:
		cache_unpin(t->cf, root);
		break;
	case FISSIBLE:
		if (locktype == L_READ) {
			cache_unpin(t->cf, root);
			locktype = L_WRITE;
			goto CHANGE_LOCK_TYPE;
		}
		_root_fissible(t, root);
		break;
	case FLUSHBLE:
		if (locktype == L_READ) {
			cache_unpin(t->cf, root);
			locktype = L_WRITE;
			goto CHANGE_LOCK_TYPE;
		}
		tree_flush_node_on_background(t, root);
		break;
	}

	return NESS_OK;
}

NID hdr_next_nid(struct tree *t)
{
	atomic64_increment(&t->hdr->last_nid);
	nassert(t->hdr->last_nid >= NID_START);

	return t->hdr->last_nid;
}

MSN hdr_next_msn(struct tree *t)
{
	atomic64_increment(&t->hdr->last_msn);

	return t->hdr->last_msn;
}

struct tree *tree_open(const char *dbname,
                       struct options *opts,
                       struct status *status,
                       struct cache *cache,
                       struct tree_callback *tcb) {
	int fd;
	int flag;
	int is_create = 0;
	mode_t mode;
	struct tree *t;
	struct node *root;
	struct cache_file *cf;

	t = xcalloc(1, sizeof(*t));
	t->opts = opts;
	t->status = status;

	mode = S_IRWXU | S_IRWXG | S_IRWXO;
	flag = O_RDWR | O_BINARY;
	if (opts->use_directio)
		fd = ness_os_open_direct(dbname, flag, mode);
	else
		fd = ness_os_open(dbname, flag, mode);

	if (fd == -1) {
		if (opts->use_directio)
			fd = ness_os_open(dbname, flag | O_CREAT, mode);
		else
			fd = ness_os_open_direct(dbname, flag | O_CREAT, mode);
		if (fd == -1)
			goto ERR;
		is_create = 1;
	}

	t->fd = fd;
	t->block = block_new(t->status);

	/* tree header */
	if (is_create) {
		t->hdr = xcalloc(1, sizeof(*t->hdr));
		t->hdr->height = 0U;
		t->hdr->last_nid = NID_START;
	} else
		tcb->fetch_hdr(t);

	t->hdr->opts = opts;

	/* create cache file */
	cf = cache_file_create(cache, tcb, t);
	t->cf = cf;

	/* tree root node */
	if (is_create) {
		cache_create_node_and_pin(cf, 0U, 0U, &root);
		leaf_alloc_buffer(root);
		root->isroot = 1;
		node_set_dirty(root);

		cache_unpin(cf, root);
		t->hdr->root_nid = root->nid;
	} else {
		/* get the root node */
		if (cache_get_and_pin(cf, t->hdr->root_nid, &root, L_READ) !=
		    NESS_OK) {
			__PANIC("get root from cache error [%" PRIu64 "]",
			        t->hdr->root_nid);
		}
		root->isroot = 1;
		cache_unpin(cf, root);
	}

	return t;

ERR:
	xfree(t);
	return NESS_ERR;
}

int tree_put(struct tree *t,
             struct msg *k,
             struct msg *v,
             msgtype_t type,
             TXN *txn)
{
	TXNID child_xid = TXNID_NONE;
	TXNID parent_xid = TXNID_NONE;

	if (txn) {
		FILENUM fn = {.fileid = t->cf->filenum};
		child_xid = txn->txnid;
		parent_xid = txn->root_parent_txnid;

		switch (type) {
		case MSG_INSERT:
			rollback_save_cmdinsert(txn, fn, k);
			break;
		case MSG_DELETE:
			rollback_save_cmddelete(txn, fn, k);
			break;
		case MSG_UPDATE:
			rollback_save_cmdupdate(txn, fn, k);
			break;
		default:
			break;
		}
	}

	struct bt_cmd cmd = {
		.msn = hdr_next_msn(t),
		.type = type,
		.key = k,
		.val = v,
		.xidpair = {.child_xid = child_xid, .parent_xid = parent_xid}
	};

	return  root_put_cmd(t, &cmd);
}

void tree_free(struct tree *t)
{
	if (!t) return;

	ness_os_close(t->fd);
	block_free(t->block);
	xfree(t->hdr);
	xfree(t);
}
