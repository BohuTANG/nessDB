/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "c.h"
#include "x.h"
#include "t.h"

/*
 *
 * PROCESS:
 *	- if the layout as follows(3 pivots, 4 partitions):
 *
 *			+--------+--------+--------+
 *	 		|   15   |   17   |   19   | +∞
 *	 		+--------+--------+--------+\
 *	 		  pidx0    pidx1    pidx2    pidx3
 *
 *
 *	- so if spk is 16, pidx = 1, after added(4 pivtos, 5 partitions):
 *
 *			+--------+--------+--------+--------+
 *	 		|   15   |  [16]  |   17   |   19   | +∞
 *	 		+--------+--------+--------+--------+\
 *	 		  pidx0   [pidx1]   pidx2    pidx3     pidx4
 *
 *
 * ENTER:
 *	- parent is already locked(L_WRITE)
 *	- node a is already locked(L_WRITE)
 *	- node b is already locked(L_WRITE)
 */
static void _add_pivot_to_parent(struct node *parent,
                                 int child_num,
                                 struct node *a,
                                 struct node *b,
                                 struct msg *spk)
{
	int i;
	int pidx;
	struct partition *part;

	pidx = child_num;
	parent->pivots = xrealloc(parent->pivots, parent->n_children * PIVOT_SIZE);
	parent->parts = xrealloc(parent->parts, (parent->n_children + 1) * PART_SIZE);

	/* slide pivots */
	for (i = (parent->n_children - 1); i > pidx; i--)
		parent->pivots[i] = parent->pivots[i - 1];

	/* slide parts */
	for (i = parent->n_children; i > pidx; i--)
		parent->parts[i] = parent->parts[i - 1];

	/* new part */
	msgcpy(&parent->pivots[pidx], spk);
	parent->parts[pidx].child_nid = a->nid;
	part = &parent->parts[pidx];
	part->msgbuf = nmb_new(parent->opts);

	parent->parts[pidx + 1].child_nid = b->nid;
	parent->n_children += 1;
	node_set_dirty(parent);
}

/*
 * EFFECT:
 *	- split the child
 *	- add a new pivot(split-key) to parent
 * ENTER:
 *	- parent is already locked(L_WRITE)
 *	- child is already locked(L_WRITE)
 * EXITS:
 *	- parent is locked
 *	- child is locked
 */
void node_split_child(struct buftree *t,
                      struct node *parent,
                      struct node *child)
{
	int child_num;
	struct node *a;
	struct node *b;
	struct msg *split_key;

	child->i->split(t, child, &a, &b, &split_key);
	child_num = node_partition_idx(parent, split_key);

	/* add pivot to parent */
	_add_pivot_to_parent(parent, child_num, a, b, split_key);
	cache_unpin(t->cf, b->cpair);
	msgfree(split_key);
}

/*
 * EFFECT:
 *	- in order to keep the root NID is eternal
 * ENTER:
 *	- old_root is already locked
 *	- new_root is already locked
 * EXITS:
 *	- old_root is already locked
 *	- new_root is already locked
 */
static void _root_swap(struct node *new_root, struct node *old_root)
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

static void _root_split(struct buftree *t,
                        struct node *new_root,
                        struct node *old_root)
{
	struct node *a;
	struct node *b;
	struct msg *split_key = NULL;

	__DEBUG("root split begin, old NID %"PRIu64" , height %d"
	        , old_root->nid
	        , old_root->height);

	old_root->i->split(t, old_root, &a, &b, &split_key);
	/* swap two roots */
	_root_swap(new_root, old_root);

	msgcpy(&new_root->pivots[0], split_key);
	new_root->parts[0].child_nid = a->nid;
	new_root->parts[1].child_nid = b->nid;
	msgfree(split_key);

	cache_unpin(t->cf, b->cpair);

	node_set_dirty(old_root);
	node_set_dirty(new_root);

	t->hdr->height++;
	__DEBUG("root split end, old NID %"PRIu64, old_root->nid);
}

/*
 * EFFECT:
 *	- split the fissible root
 */
void _root_fissible(struct buftree *t, struct node *root)
{
	struct node *new_root;
	uint32_t new_root_height = 1;
	uint32_t new_root_children = 2;

	/* alloc a nonleaf node with 2 children */
	NID nid = hdr_next_nid(t->hdr);
	inter_new(t->hdr,
	          nid,
	          new_root_height,
	          new_root_children,
	          &new_root);
	new_root->i->init_msgbuf(new_root);

	cache_put_and_pin(t->cf, nid, new_root);

	_root_split(t, new_root, root);

	cache_unpin(t->cf, root->cpair);
	cache_unpin(t->cf, new_root->cpair);
}

/*
 * PROCESS:
 *	- put cmd to root
 *	- check root reactivity
 *	    -- FISSIBLE: split root
 *	    -- FLUSHABLE: flush root in background
 * ENTER:
 *	- no nodes are locked
 * EXITS:
 *	- all nodes are unlocked
 */
int root_put_cmd(struct buftree *t, struct bt_cmd *cmd)
{
	struct node *root;
	enum node_state state;
	volatile int hasput = 0;
	enum lock_type locktype = L_READ;

CHANGE_LOCK_TYPE:
	if (!cache_get_and_pin(t->cf, t->hdr->root_nid, (void**)&root, locktype))
		return NESS_ERR;

	if (!hasput) {
		root->i->put(root, cmd);
		hasput = 1;
	}

	state = get_node_state(root);
	switch (state) {
	case STABLE:
		cache_unpin(t->cf, root->cpair);
		break;
	case FISSIBLE:
		if (locktype == L_READ) {
			cache_unpin(t->cf, root->cpair);
			locktype = L_WRITE;
			goto CHANGE_LOCK_TYPE;
		}
		_root_fissible(t, root);
		break;
	case FLUSHBLE:
		if (locktype == L_READ) {
			cache_unpin(t->cf, root->cpair);
			locktype = L_WRITE;
			goto CHANGE_LOCK_TYPE;
		}
		buftree_flush_node_on_background(t, root);
		break;
	}

	return NESS_OK;
}

void options_init(struct tree_options *opts)
{
	// root
	opts->root_node_fanout = BT_DEFAULT_ROOT_FANOUT;
	opts->root_node_size = BT_DEFAULT_ROOT_NODE_SIZE;

	// inter
	opts->inner_node_fanout = BT_DEFAULT_INTER_FANOUT;
	opts->inner_node_size = BT_DEFAULT_INTER_NODE_SIZE;

	// leaf
	opts->leaf_node_size = BT_DEFAULT_LEAF_NODE_SIZE;
	opts->leaf_basement_size = BT_DEFAULT_LEAF_BASEMENT_SIZE;

	// flag
	opts->flag = O_RDWR ;
	opts->mode = S_IRWXU | S_IRWXG | S_IRWXO;

	// IO
	opts->use_directio = 1;
	opts->enable_redo_log = 1;
	opts->vfs = &ness_osvfs;

	// others
	opts->compress_method = NESS_SNAPPY_METHOD;
	opts->bt_compare_func = &bt_compare_func_builtin;
}

struct buftree *buftree_open(const char *dbname, struct cache *cache)
{
	int fd;
	int is_create = 0;
	struct buftree *t;
	struct node *root;
	struct cache_file *cf;
	struct tree_options *opts;
	struct tree_operations *tree_opts = &buftree_operations;

	// options
	opts = xcalloc(1, sizeof(*opts));
	options_init(opts);

	t = xcalloc(1, sizeof(*t));
	if (opts->use_directio) {
#ifdef HAVE_DIRECTIO
		opts->flag |= O_DIRECT;
#endif
	}
	fd = opts->vfs->open(dbname, opts->flag, opts->mode);
	if (fd == -1) {
		fd = opts->vfs->open(dbname, opts->flag | O_CREAT, opts->mode);
		if (fd == -1)
			goto ERR;
		is_create = 1;
	}

	t->fd = fd;
	t->hdr = hdr_new(opts);

	/* tree header */
	if (!is_create) {
		tree_opts->fetch_hdr(fd, t->hdr);
	}

	/* create cache file */
	cf = cache_file_create(cache, t->fd, t->hdr, tree_opts);
	t->cf = cf;

	/* tree root node */
	if (is_create) {
		NID nid = hdr_next_nid(t->hdr);
		leaf_new(t->hdr, nid, 0, 1, &root);
		root->i->init_msgbuf(root);

		cache_put_and_pin(cf, nid, root);
		root->isroot = 1;
		node_set_dirty(root);

		cache_unpin(cf, root->cpair);
		t->hdr->root_nid = root->nid;
		__DEBUG("create new root, NID %"PRIu64, root->nid);
	} else {
		/* get the root node */
		if (cache_get_and_pin(cf, t->hdr->root_nid, (void**)&root, L_READ) != NESS_OK)
			__PANIC("get root from cache error [%" PRIu64 "]", t->hdr->root_nid);

		root->isroot = 1;
		cache_unpin(cf, root->cpair);
		__DEBUG("fetch root, NID %"PRIu64, root->nid);
	}

	return t;

ERR:
	xfree(t);
	return NESS_ERR;
}

int buftree_put(struct buftree *t,
                struct msg *k,
                struct msg *v,
                msgtype_t type,
                TXN *txn)
{
	TXNID child_xid = TXNID_NONE;
	TXNID parent_xid = TXNID_NONE;

	if (txn) {
		int fn = t->cf->filenum;
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
		.msn = hdr_next_msn(t->hdr),
		.type = type,
		.key = k,
		.val = v,
		.xidpair = {.child_xid = child_xid, .parent_xid = parent_xid}
	};

	return  root_put_cmd(t, &cmd);
}

void buftree_set_compress_method(struct hdr *hdr, int compress_method)
{
	hdr->opts->compress_method = compress_method;
}

void buftree_set_node_fanout(struct hdr *hdr, int fanout)
{
	hdr->opts->inner_node_fanout = fanout;
}

void buftree_free(struct buftree *t)
{
	if (!t) return;
	struct tree_options *opts;

	opts = t->hdr->opts;
	/* flush dirty nodes&hdr to disk */
	cache_file_flush_dirty_nodes(t->cf);
	cache_file_flush_hdr(t->cf);
	cache_file_free(t->cf);
	opts->vfs->close(t->fd);
	xfree(opts);
	hdr_free(t->hdr);
	xfree(t);
}
