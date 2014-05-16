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
	uint64_t tree_leaf_nums;
	uint64_t tree_nonleaf_split_nums;
	uint64_t tree_nonleaf_put_nums;
	uint64_t tree_nonleaf_nums;
	uint64_t tree_flush_child_nums;
	uint64_t tree_flush_child_costs;
	uint64_t tree_root_new_nums;
	uint64_t tree_add_pivots_nums;

	uint64_t tree_node_fetch_nums;
	uint64_t tree_node_fetch_costs;
	uint64_t tree_node_flush_nums;
	uint64_t tree_node_flush_costs;

	uint64_t leaf_read_from_disk_costs;
	uint64_t nonleaf_read_from_disk_costs;
};

static inline struct status *status_new() {
	struct status *status;

	status = xcalloc(1, sizeof(*status));

	return status;
}

static inline void status_free(struct status *status)
{
#if 1
	printf("--status:\n");
	printf("\ttree_nodes: leaf [%" PRIu64 "], nonleaf [%" PRIu64 "]\n",
	       status->tree_leaf_nums,
	       status->tree_nonleaf_nums);

	printf("\tleaf_read_from_disk_costs: [%" PRIu64 "]\n", status->leaf_read_from_disk_costs);
	printf("\tnonleaf_read_from_disk_costs: [%" PRIu64 "]\n", status->nonleaf_read_from_disk_costs);

	printf("\ttree_leaf_split_nums: [%" PRIu64 "]\n", status->tree_leaf_split_nums);
	printf("\ttree_nonleaf_split_nums: [%" PRIu64 "]\n", status->tree_nonleaf_split_nums);

	printf("\ttree_leaf_put_nums: [%" PRIu64 "]\n", status->tree_leaf_put_nums);
	printf("\ttree_nonleaf_put_nums: [%" PRIu64 "]\n", status->tree_nonleaf_put_nums);

	printf("\ttree_root_new_nums: [%" PRIu64 "]\n", status->tree_root_new_nums);
	printf("\ttree_flush_child_nums: [%" PRIu64 "]\n", status->tree_flush_child_nums);
	printf("\ttree_flush_child_costs: [%" PRIu64"]\n", status->tree_flush_child_costs);
	printf("\t                      : [%.1f ms/num]\n",
	       (double) status->tree_flush_child_costs / status->tree_flush_child_nums);

	printf("\ttree_node_fetch_nums: [%" PRIu64 "]\n", status->tree_node_fetch_nums);
	printf("\ttree_node_fetch_costs: [%" PRIu64 "]\n", status->tree_node_fetch_costs);
	printf("\t                     : [%.1f ms/num]\n",
	       (double) status->tree_node_fetch_costs / status->tree_node_fetch_nums);

	printf("\ttree_node_flush_nums: [%" PRIu64 "]\n", status->tree_node_flush_nums);
	printf("\ttree_node_flush_costs: [%" PRIu64 "]\n", status->tree_node_flush_costs);
	printf("\t                     : [%.1f ms/num]\n",
	       (double) status->tree_node_flush_costs / status->tree_node_flush_nums);
#endif
	xfree(status);
}

#endif /* nessDB_STATUS_H_ */
