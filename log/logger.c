/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "logger.h"


struct logger *logger_new(struct cache *cache, struct txnmgr *txnmgr) {
	struct logger *lger;

	lger = xcalloc(1, sizeof(*lger));
	lger->cache = cache;
	lger->txnmgr = txnmgr;

	return lger;
}

void logger_free(struct logger *logger)
{
	xfree(logger);
}
