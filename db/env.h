/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_ENV_H_
#define nessDB_ENV_H_

struct env {
	/* cache */
	uint64_t cache_limits_bytes;
	uint64_t cache_high_watermark;
	uint32_t cache_flush_period_ms;
	uint32_t cache_checkpoint_period_ms;

	/* IO */
	char *redo_path;
	char *dir;

	/* internal  */
	struct status *status;
	struct cache *cache;
	struct txnmgr *txnmgr;

	/* flags */
	uint32_t flags;
};

struct env *env_open(const char *home, uint32_t flags);
void env_close(struct env *e);
int env_set_cache_size(struct env *e, uint64_t cache_size);
int env_set_compress_method(struct env *e, int method);
int env_set_compare_func(struct env *e, int (*bt_compare_func)(void *a, int lena, void *b, int lenb));

#endif /* nessDB_ENV_H_ */
