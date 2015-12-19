/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_NODE_H_
#define nessDB_NODE_H_

#define PART_SIZE (sizeof(struct partition))
#define PIVOT_SIZE (sizeof(struct msg))


/* lock type */
enum lock_type {
	L_NONE = 0,
	L_READ = 1,
	L_WRITE = 2,
};

enum node_state {
	FISSIBLE,
	FLUSHBLE,
	STABLE
};

struct ancestors {
	int level;
	int childnum;
	void *v;
	struct ancestors *next;
};

struct disk_info {
	uint32_t xsum;
	uint32_t start;
	uint32_t compressed_size;
	uint32_t uncompressed_size;

	char *compressed_ptr;
	char *uncompressed_ptr;
};

struct partition {
	int fetched;
	NID child_nid;
	void *msgbuf;
	struct disk_info disk_info;
};

struct node_attr {
	uint64_t newsz;
	uint64_t oldsz;
	ness_mutex_t mtx;
};

struct node {
	MSN msn;
	NID nid;
	int dead;
	int dirty;
	int isroot;
	int height;
	int layout_version;
	int skeleton_size;
	enum lock_type pintype;

	struct node_attr attr;
	struct timespec modified;

	int n_children;
	struct msg *pivots;
	struct partition *parts;
	struct cpair *cpair;

	struct tree_options *opts;
	struct node_operations *i;
};

struct node_operations {
	void (*apply)(struct node *node,
	              struct nmb *buffer,
	              struct msg *left,
	              struct msg *right);
	void (*split)(void *tree,
	              struct node *node,
	              struct node **a,
	              struct node **b,
	              struct msg **split_key);
	int (*put)(struct node *node, struct bt_cmd *cmd);
	void (*init_msgbuf)(struct node *node);
	void (*free)(struct node *node);
	int (*find_heaviest)(struct node *node);
	uint32_t (*size)(struct node *node);
	uint32_t (*count)(struct node *node);

	void (*mb_packer)(void *packer, void *msgbuf);
	void (*mb_unpacker)(void *packer, void *msgbuf);
} NESSPACKED;

int node_is_root(struct node *node);
void node_set_dirty(struct node *node);
void node_set_nondirty(struct node *node);
int node_is_dirty(struct node *node);
int node_partition_idx(struct node *node, struct msg *k);
enum node_state get_node_state(struct node *node);

#endif /* nessDB_NODE_H_ */
