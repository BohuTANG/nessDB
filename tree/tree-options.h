/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_OPTIONS_H_
#define nessDB_OPTIONS_H_

struct tree_options {
	/* tree */
	uint32_t root_node_fanout;
	uint32_t root_node_size;
	uint32_t inner_node_fanout;
	uint32_t inner_node_size;
	uint32_t leaf_node_size;
	uint32_t leaf_basement_size;

	uint32_t enable_redo_log;
	int compress_method;

	int flag;
	mode_t mode;
	uint8_t use_directio;

	/* callback */
	int (*bt_compare_func)(void *a, int alen, void *b, int blen);
	struct ness_vfs *vfs;
};

#endif /* nessDB_ENV_H_ */
