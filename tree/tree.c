/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
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
static void _add_pivot_to_parent(struct tree *t,
                                 struct node *parent,
                                 int child_num,
                                 struct node *a,
                                 struct node *b,
                                 struct msg *spk)
{
	int i;
	int pidx;
	struct child_pointer *cptr;

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
	cptr = &parent->parts[pidx].ptr;
	cptr->u.nonleaf = create_nonleaf(t->e);

	parent->parts[pidx + 1].child_nid = b->nid;
	parent->n_children += 1;
	node_set_dirty(parent);

	status_increment(&t->e->status->tree_add_pivots_nums);
}

/*
 * EFFECT:
 *	- split leaf&lmb into two leaves:a & b
 *	  a&b are both the half of the lmb
 *
 * PROCESS:
 *	- leaf:
 *		+-----------------------------------+
 *		|  0  |  1  |  2  |  3  |  4  |  5  |
 *		+-----------------------------------+
 *
 *	- split:
 *				   root
 *				 +--------+
 *				 |   2    |
 *				 +--------+
 *	  	                /          \
 *	    	+-----------------+	 +------------------+
 *	    	|  0  |  1  |  2  |	 |  3  |  4  |  5   |
 *	    	+-----------------+	 +------------------+
 *	    	      nodea			nodeb
 *
 * ENTER:
 *	- leaf is already locked (L_WRITE)
 * EXITS:
 *	- a is locked
 *	- b is locked
 */
static void _leaf_and_lmb_split(struct tree *t,
                                struct node *leaf,
                                struct node **a,
                                struct node **b,
                                struct msg **split_key)
{
	struct child_pointer *cptra;
	struct child_pointer *cptrb;
	struct node *leafa;
	struct node *leafb;
	struct lmb *mb;
	struct lmb *mba;
	struct lmb *mbb;
	struct msg *sp_key = NULL;

	__DEBUG("leaf split begin, NID %"PRIu64""
	        ", nodesz %d"
	        ", nodec %d"
	        ", children %d"
	        , leaf->nid
	        , node_size(leaf)
	        , node_count(leaf)
	        , leaf->n_children);

	leafa = leaf;
	cptra = &leafa->parts[0].ptr;

	/* split lmb of leaf to mba & mbb */
	mb = cptra->u.leaf->buffer;
	lmb_split(mb, &mba, &mbb, &sp_key);
	lmb_free(mb);

	/* reset leafa buffer */
	cptra->u.leaf->buffer = mba;

	/* new leafb */
	NID nid = hdr_next_nid(t->hdr);
	node_create(nid, 0, 1, t->hdr->version, t->e, &leafb);
	cache_put_and_pin(t->cf, nid, leafb);

	cptrb = &leafb->parts[0].ptr;
	lmb_free(cptrb->u.leaf->buffer);
	cptrb->u.leaf->buffer = mbb;

	/* set dirty */
	node_set_dirty(leafa);
	node_set_dirty(leafb);

	__DEBUG("leaf split end, leafa NID %"PRIu64""
	        ", nodesz %d"
	        ", nodec %d"
	        ", children %d"
	        , leafa->nid
	        , node_size(leafa)
	        , node_count(leafa)
	        , leafa->n_children);

	__DEBUG("leaf split end, leafb NID %"PRIu64""
	        ", nodesz %d"
	        ", nodec %d"
	        ", children %d"
	        , leafb->nid
	        , node_size(leafb)
	        , node_count(leafb)
	        , leafb->n_children);

	*a = leafa;
	*b = leafb;
	*split_key = sp_key;
	status_increment(&t->e->status->tree_leaf_split_nums);
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
 * ENTER:
 *	- node is already locked(L_WRITE)
 * EXITS:
 *	- a is locked(L_WRITE)
 *	- b is locked(L_WRITE)
 */
static void _node_split(struct tree *t,
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

	__DEBUG("nonleaf split begin, NID %"PRIu64""
	        ", nodesz %d"
	        ", nodec %d"
	        ", children %d"
	        , node->nid
	        , node_size(node)
	        , node_count(node)
	        , node->n_children);

	nodea = node;
	pivots_old = node->n_children - 1;
	nassert(pivots_old > 2);

	pivots_in_a = pivots_old / 2;
	pivots_in_b = pivots_old - pivots_in_a;

	/* node a */
	nodea->n_children = pivots_in_a + 1;

	/* node b */
	NID nid = hdr_next_nid(t->hdr);
	node_create_light(nid, node->height > 0 ? 1 : 0, pivots_in_b + 1, t->hdr->version, t->e, &nodeb);
	cache_put_and_pin(t->cf, nid, nodeb);

	for (i = 0; i < (pivots_in_b); i++)
		nodeb->pivots[i] = nodea->pivots[pivots_in_a + i];

	for (i = 0; i < (pivots_in_b + 1); i++)
		nodeb->parts[i] = nodea->parts[pivots_in_a + i];

	/* the rightest partition of nodea */
	struct child_pointer *ptr = &nodea->parts[pivots_in_a].ptr;

	if (nodea->height > 0)
		ptr->u.nonleaf = create_nonleaf(t->e);
	else
		ptr->u.leaf = create_leaf(t->e);


	/* split key */
	spk = msgdup(&node->pivots[pivots_in_a - 1]);

	node_set_dirty(nodea);
	node_set_dirty(nodeb);

	__DEBUG("nonleaf split end, nodea NID %"PRIu64""
	        ", nodesz %d"
	        ", nodec %d"
	        ", children %d"
	        , nodea->nid
	        , node_size(nodea)
	        , node_count(nodea)
	        , nodea->n_children);

	__DEBUG("nonleaf split end, nodeb NID %"PRIu64""
	        ", nodesz %d"
	        ", nodec %d"
	        ", children %d"
	        , nodeb->nid
	        , node_size(nodeb)
	        , node_count(nodeb)
	        , nodeb->n_children);

	*a = nodea;
	*b = nodeb;
	*split_key = spk;
}

struct cpair_attr make_cpair_attr(struct node *n) {
	struct cpair_attr attr = {.nodesz = node_size(n)};

	return attr;
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
void node_split_child(struct tree *t,
                      struct node *parent,
                      struct node *child)
{
	int child_num;
	struct node *a;
	struct node *b;
	struct msg *split_key;

	if (child->height > 0 || child->n_children > 2)
		_node_split(t, child, &a, &b, &split_key);
	else
		_leaf_and_lmb_split(t, child, &a, &b, &split_key);

	child_num = node_partition_idx(parent, split_key);

	/* add pivot to parent */
	_add_pivot_to_parent(t, parent, child_num, a, b, split_key);
	cache_unpin(t->cf, b->cpair, make_cpair_attr(b));
	msgfree(split_key);

	if (child->height > 0)
		status_increment(&t->e->status->tree_nonleaf_split_nums);
	else
		status_increment(&t->e->status->tree_leaf_split_nums);
}

enum reactivity get_reactivity(struct tree *t, struct node *node)
{
	uint32_t children = node->n_children;

	if (nessunlikely(node->height == 0)) {
		if (node_size(node) >= t->e->leaf_default_node_size)
			return FISSIBLE;
	} else {
		if (children >= t->e->inner_node_fanout)
			return FISSIBLE;

		if (node_size(node) >= t->e->inner_default_node_size)
			return FLUSHBLE;
	}

	return STABLE;
}

/*
 * EFFECT:
 *	- put cmd to leaf
 * ENTER:
 *	- leaf is already locked(L_WRITE)
 * EXITS:
 *	- leaf is locked
 */
void leaf_put_cmd(struct node *leaf, struct bt_cmd *cmd)
{
	leaf_apply_msg(leaf, cmd);
	leaf->msn = cmd->msn > leaf->msn ? cmd->msn : leaf->msn;
	node_set_dirty(leaf);
}

/*
 * EFFECT:
 *	- put cmd to nonleaf
 * ENTER:
 *	- node is already locked (L_READ)
 * EXITS:
 *	- node is locked
 */
void nonleaf_put_cmd(struct node *node, struct bt_cmd *cmd)
{
	uint32_t pidx;
	struct nmb *buffer;

	pidx = node_partition_idx(node, cmd->key);
	buffer = node->parts[pidx].ptr.u.nonleaf->buffer;

	nassert(buffer);
	nmb_put(buffer,
	        cmd-> msn,
	        cmd->type,
	        cmd->key,
	        cmd->val,
	        &cmd->xidpair);
	node->msn = cmd->msn > node->msn ? cmd->msn : node->msn;
	node_set_dirty(node);
}

/*
 * EFFECT:
 *	- put cmd to node
 * ENTER:
 *	- node is already locked(L_WRITE)
 * EXITS:
 *	- node is locked
 */
void node_put_cmd(struct tree *t, struct node *node, struct bt_cmd *cmd)
{
	if (nessunlikely(node->height == 0)) {
		leaf_put_cmd(node, cmd);
		status_increment(&t->e->status->tree_leaf_put_nums);
	} else {
		nonleaf_put_cmd(node, cmd);
		status_increment(&t->e->status->tree_nonleaf_put_nums);
	}
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

static void _root_split(struct tree *t,
                        struct node *new_root,
                        struct node *old_root)
{
	struct node *a;
	struct node *b;
	struct msg *split_key = NULL;

	__DEBUG("root split begin, old NID %"PRIu64" , height %d"
	        , old_root->nid
	        , old_root->height);

	if (old_root->height > 0 || old_root->n_children > 2)
		_node_split(t, old_root, &a, &b, &split_key);
	else
		_leaf_and_lmb_split(t, old_root, &a, &b, &split_key);

	/* swap two roots */
	_root_swap(new_root, old_root);

	msgcpy(&new_root->pivots[0], split_key);
	new_root->parts[0].child_nid = a->nid;
	new_root->parts[1].child_nid = b->nid;
	msgfree(split_key);

	cache_unpin(t->cf, b->cpair, make_cpair_attr(b));

	node_set_dirty(old_root);
	node_set_dirty(new_root);

	t->hdr->height++;
	status_increment(&t->e->status->tree_root_new_nums);
	__DEBUG("root split end, old NID %"PRIu64, old_root->nid);
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

	/* alloc a nonleaf node with 2 children */
	NID nid = hdr_next_nid(t->hdr);
	node_create(nid, new_root_height, new_root_children, t->hdr->version, t->e, &new_root);
	cache_put_and_pin(t->cf, nid, new_root);

	_root_split(t, new_root, root);

	cache_unpin(t->cf, root->cpair, make_cpair_attr(root));
	cache_unpin(t->cf, new_root->cpair, make_cpair_attr(new_root));
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
int root_put_cmd(struct tree *t, struct bt_cmd *cmd)
{
	struct node *root;
	enum reactivity re;
	volatile int hasput = 0;
	enum lock_type locktype = L_READ;

CHANGE_LOCK_TYPE:
	if (!cache_get_and_pin(t->cf, t->hdr->root_nid, (void**)&root, locktype))
		return NESS_ERR;

	if (!hasput) {
		node_put_cmd(t, root, cmd);
		hasput = 1;
	}

	re = get_reactivity(t, root);
	switch (re) {
	case STABLE:
		cache_unpin(t->cf, root->cpair, make_cpair_attr(root));
		break;
	case FISSIBLE:
		if (locktype == L_READ) {
			cache_unpin(t->cf, root->cpair, make_cpair_attr(root));
			locktype = L_WRITE;
			goto CHANGE_LOCK_TYPE;
		}
		_root_fissible(t, root);
		break;
	case FLUSHBLE:
		if (locktype == L_READ) {
			cache_unpin(t->cf, root->cpair, make_cpair_attr(root));
			locktype = L_WRITE;
			goto CHANGE_LOCK_TYPE;
		}
		tree_flush_node_on_background(t, root);
		break;
	}

	return NESS_OK;
}

struct tree *tree_open(const char *dbname,
                       struct env *e,
                       struct tree_callback *tcb) {
	int fd;
	int flag;
	mode_t mode;
	int is_create = 0;
	struct tree *t;
	struct node *root;
	struct cache_file *cf;

	t = xcalloc(1, sizeof(*t));
	t->e = e;

	mode = S_IRWXU | S_IRWXG | S_IRWXO;
	flag = O_RDWR | O_BINARY;
	if (e->use_directio)
		fd = ness_os_open_direct(dbname, flag, mode);
	else
		fd = ness_os_open(dbname, flag, mode);

	if (fd == -1) {
		if (e->use_directio)
			fd = ness_os_open(dbname, flag | O_CREAT, mode);
		else
			fd = ness_os_open_direct(dbname, flag | O_CREAT, mode);
		if (fd == -1)
			goto ERR;
		is_create = 1;
	}

	t->fd = fd;
	t->hdr = hdr_new(e);

	/* tree header */
	if (!is_create) {
		tcb->fetch_hdr_cb(fd, t->hdr);
	}

	/* create cache file */
	cf = cache_file_create(e->cache, t->fd, t->hdr, tcb);
	t->cf = cf;

	/* tree root node */
	if (is_create) {
		NID nid = hdr_next_nid(t->hdr);
		node_create(nid, 0, 1, t->hdr->version, t->e, &root);
		cache_put_and_pin(cf, nid, root);
		root->isroot = 1;
		node_set_dirty(root);

		cache_unpin(cf, root->cpair, make_cpair_attr(root));
		t->hdr->root_nid = root->nid;
		__DEBUG("create new root, NID %"PRIu64, root->nid);
	} else {
		/* get the root node */
		if (cache_get_and_pin(cf, t->hdr->root_nid, (void**)&root, L_READ) != NESS_OK)
			__PANIC("get root from cache error [%" PRIu64 "]", t->hdr->root_nid);

		root->isroot = 1;
		cache_unpin(cf, root->cpair, make_cpair_attr(root));
		__DEBUG("fetch root, NID %"PRIu64, root->nid);
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

void tree_free(struct tree *t)
{
	if (!t) return;

	/* flush dirty nodes&hdr to disk */
	cache_file_flush_dirty_nodes(t->cf);
	cache_file_flush_hdr(t->cf);
	cache_file_free(t->cf);
	hdr_free(t->hdr);

	ness_os_close(t->fd);
	xfree(t);
}
