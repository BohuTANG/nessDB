/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "c.h"
#include "t.h"
#include "x.h"

struct env *env_open(const char *home, uint32_t flags)
{
	struct env *e;

	e = xcalloc(1, sizeof(*e));
	e->flags = flags;

	/* cache */
	e->cache_limits_bytes = 1024 * 1024 * 1024;
	e->cache_flush_period_ms = 100;		/* 0.1s */
	e->cache_checkpoint_period_ms = 600000;	/* 60s */

	/* IO */
	e->redo_path = "./dbbench";
	if (!home)
		home = ".";

	e->dir = xcalloc(1, strlen(home) + 1);
	xmemcpy(e->dir, (void*)home, strlen(home));
	ness_check_dir(e->dir);

	/* internal */
	e->cache = cache_new(e);
	nassert(e->cache);

	e->txnmgr = txnmgr_new();
	nassert(e->txnmgr);

	e->status = status_new();
	nassert(e->status);

	return e;
}

void env_close(struct env *e)
{
	cache_free(e->cache);
	status_free(e->status);
	txnmgr_free(e->txnmgr);
	xfree(e->dir);
	xfree(e);
}

int env_set_cache_size(struct env *e, uint64_t cache_size)
{
	struct cache *c = e->cache;

	e->cache_limits_bytes = cache_size;
	quota_change_limit(c->quota, cache_size);

	return NESS_OK;
}

int env_set_compress_method(struct env *e, int method)
{
	(void)e;
	if (method >= (int)sizeof(ness_compress_method_t))
		return NESS_ERR;

	return NESS_ERR;
}

int env_set_compare_func(struct env *e, int (*bt_compare_func)(void *a, int lena, void *b, int lenb))
{
	(void)e;
	(void)bt_compare_func;
	// set db's compare func

	return NESS_OK;
}
