/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_TCURSOR_H_
#define nessDB_TCURSOR_H_

#include "internal.h"
#include "node.h"
#include "tree.h"
#include "msg.h"

/**
 * @file cursor.h
 * @brief cursor of tree
 *
 * supports: SEEK, SEEKTOFIRST, SEEKTOLAST, NEXT, PREV
 * when we do the range seek operations,
 * we first get a root-to-leaf path with READ-LOCK
 */

struct search;
typedef int (*search_direction_compare_func)(struct search *, struct msg *);

/* pivot compare function, default is msgcmp */
typedef int (*search_pivotbound_compare_func)(struct search *, struct msg *);

/* key compare function, default is msgcmp */
typedef int (*search_key_compare_func)(struct msg *, struct msg *);

typedef enum {
	SLIP_POSI = +1,	/* goto next if equal */
	SLIP_ZERO = 0,	/* stay origin if equal */
	SLIP_NEGA = -1	/* goto prev if equal */
} slip_t;

typedef enum {
	SEARCH_FORWARD = 1,
	SEARCH_BACKWARD = -1
} direction_t;

enum {
	CURSOR_CONTINUE = 10000,
	CURSOR_EOF = -10001,
	CURSOR_TRY_AGAIN = -10002
};

/* internal search entry */
struct search {
	struct msg *key;
	struct msg *pivot_bound;
	slip_t slip;
	direction_t direction;
	search_direction_compare_func direction_compare_func;
	search_pivotbound_compare_func pivotbound_compare_func;
	search_key_compare_func key_compare_func;
};

struct cursor {
	int valid;
	MSN msn;
	struct msg key;		/* return key */
	struct msg val;		/* return val */
	msgtype_t msgtype;
	struct tree *tree;	/* index */
	void *extra;		/* extra(no-use) */

	/* root->to->leaf bsm path */
	int ances_size;
	struct ancestors *ances;
};

struct cursor *cursor_new(struct tree *t);
void cursor_free(struct cursor *cur);
int tree_cursor_valid(struct cursor *cur);
void tree_cursor_first(struct cursor *cur);
void tree_cursor_last(struct cursor *cur);
void tree_cursor_next(struct cursor *cur);
void tree_cursor_prev(struct cursor *cur);
void tree_cursor_current(struct cursor *cur);

void ancestors_append(struct cursor *cur, struct basement *bsm);

#endif /* _nessDB_TCURSOR_H_ */
