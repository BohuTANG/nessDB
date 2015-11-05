/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_NODE_H_
#define nessDB_NODE_H_

#define PART_SIZE (sizeof(struct partition))
#define PIVOT_SIZE (sizeof(struct msg))

enum reactivity {
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

/* funcations for nodes operations */
struct node_operations {
	int (*update_func)(void *a, void *b);
	int (*delete_all_func)(void *a, void *b);
	int (*pivot_compare_func)(struct msg *a, struct msg *b);
};

struct partition_disk_info {
	uint32_t xsum;
	uint32_t start;
	uint32_t compressed_size;
	uint32_t uncompressed_size;

	char *compressed_ptr;
	char *uncompressed_ptr;
};

struct leaf_basement_node {
	MSN max_applied_msn;
	struct lmb *buffer;
};

struct nonleaf_childinfo {
	struct nmb *buffer;
};

struct child_pointer {
	union {
		struct nonleaf_childinfo *nonleaf;
		struct leaf_basement_node *leaf;
	} u;
};

struct partition {
	int fetched;
	NID child_nid;
	struct child_pointer ptr;
	struct partition_disk_info disk_info;
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
	struct node_operations *node_op;

	int n_children;
	struct msg *pivots;
	struct partition *parts;
	struct cpair *cpair;
	struct env *env;
};

struct leaf_basement_node *create_leaf(struct env *);
struct nonleaf_childinfo *create_nonleaf(struct env *);
struct node *node_alloc_empty(NID nid, int height, struct env *);
struct node *node_alloc_full(NID nid, int height, int children, int layout_version, struct env *);
void node_init_empty(struct node *node, int children, int version);
void node_ptrs_alloc(struct node *node);
void node_free(struct node *n);

void node_set_dirty(struct node *n);
void node_set_nondirty(struct node *n);
int node_is_dirty(struct node *n);

uint32_t node_size(struct node *n);
uint32_t node_count(struct node *n);

int node_partition_idx(struct node *node, struct msg *k);
int node_find_heaviest_idx(struct node *node);

int node_create(NID nid,
                uint32_t height,
                uint32_t children,
                int version,
                struct env *e,
                struct node **n);

int node_create_light(NID nid,
                      uint32_t height,
                      uint32_t children,
                      int version,
                      struct env *e,
                      struct node **n);

#endif /* nessDB_NODE_H_ */
