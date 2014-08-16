/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_ENV_H_
#define nessDB_ENV_H_

#include <stdint.h>

struct env {
	/* tree */
	uint32_t inner_node_fanout;
	uint32_t inner_default_node_size;
	uint32_t leaf_default_node_size;
	uint32_t leaf_default_basement_size;

	/* cache */
	uint64_t cache_limits_bytes;
	uint64_t cache_high_watermark;
	uint32_t cache_flush_period_ms;
	uint32_t cache_checkpoint_period_ms;

	/* db */
	uint32_t enable_redo_log;
	int compress_method;

	/* IO */
	uint8_t use_directio;
	char *redo_path;
	char *dir;

	/* internal  */
	struct status *status;
	struct cache *cache;
	struct txnmgr *txnmgr;

	/* flags */
	uint32_t flags;

	/* callback */
	int (*bt_compare_func)(void *a, int alen, void *b, int blen);
};

#endif /* nessDB_ENV_H_ */
