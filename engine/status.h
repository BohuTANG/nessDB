/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_STATUS_H_
#define nessDB_STATUS_H_

#include "xmalloc.h"

/*
 * some probes
 */
struct status {
	uint64_t tree_leaf_split_nums;
	uint64_t tree_leaf_put_nums;
	uint64_t tree_nonleaf_split_nums;
	uint64_t tree_nonleaf_put_nums;
	uint64_t tree_nonleaf_flush_nums;
	uint64_t tree_root_new_nums;
	uint64_t tree_add_pivots_nums;
	uint64_t tree_flush_child_nums;
};

static inline struct status *status_new()
{
	struct status *status;

	status = xcalloc(1, sizeof(*status));

	return status;
}

static inline void status_free(struct status *status)
{
#if 1
	printf("--status:\n");
	printf("\ttree_leaf_split_nums: [%" PRIu64 "]\n", status->tree_leaf_split_nums);
	printf("\ttree_nonleaf_split_nums: [%" PRIu64 "]\n", status->tree_nonleaf_split_nums);

	printf("\ttree_leaf_put_nums: [%" PRIu64 "]\n", status->tree_leaf_put_nums);
	printf("\ttree_nonleaf_put_nums: [%" PRIu64 "]\n", status->tree_nonleaf_put_nums);

	printf("\ttree_root_new_nums: [%" PRIu64 "]\n", status->tree_root_new_nums);
	printf("\ttree_flush_child_nums: [%" PRIu64 "]\n", status->tree_flush_child_nums);
#endif
	xfree(status);
}

#endif /* nessDB_STATUS_H_ */
