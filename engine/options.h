/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_OPTIONS_H_
#define nessDB_OPTIONS_H_

#include "xmalloc.h"

struct options {
	uint32_t inner_node_fanout;
	uint32_t inner_node_page_size;
	uint32_t inner_node_page_count;
	uint32_t leaf_node_page_size;
	uint32_t leaf_node_page_count;

	uint64_t cache_limits_bytes;
	uint64_t cache_high_watermark;
	uint32_t cache_flush_period_ms;
	uint32_t cache_checkpoint_period_ms;
	uint32_t db_memtable_count;
	uint32_t enable_redo_log;
	ness_compress_method_t compress_method;

	/* IO */
	uint8_t use_directio;
	char *redo_path;
};

static inline struct options *options_new(void) {
	struct options *opts;

	opts = xcalloc(1, sizeof(*opts));

	opts->inner_node_fanout = 16;
	opts->inner_node_page_size = 4 << 20;		/* 4MB */
	opts->inner_node_page_count = -1;
	opts->leaf_node_page_size = 1 << 20;		/* 1MB */
	opts->leaf_node_page_count = -1;

	opts->cache_limits_bytes = 512 << 20;
	opts->cache_high_watermark = 80;		/* 80% */
	opts->cache_flush_period_ms = 100;		/* 0.1s */
	opts->cache_checkpoint_period_ms = 600000;	/* 60s */

	/* IO */
	opts->use_directio = 1;
	opts->redo_path = "./dbbench";
	opts->db_memtable_count = 2;
	opts->enable_redo_log = 1;

	/* compress */
	opts->compress_method = NESS_QUICKLZ_METHOD;

	return opts;
}

static inline void options_free(struct options *opts)
{
	xfree(opts);
}

#endif /* nessDB_OPTIONS_H_ */
