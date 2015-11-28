/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_LOGGER_H_
#define nessDB_LOGGER_H_

typedef struct logger {
	struct cache *cache;
	struct txnmgr *txnmgr;
} LOGGER;

struct logger *logger_new(struct cache *cache, struct txnmgr *txnmgr);
void logger_free(struct logger *logger);

#endif /* nessDB_LOGGER_H_*/
