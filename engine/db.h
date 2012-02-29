/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _DB_H
#define _DB_H

#include <time.h>
#include "llru.h"
#include "index.h"
#include "buffer.h"
#include "util.h"
#include "config.h"

struct nessdb {
	struct llru *lru;
	struct index *idx_group[NESSDB_MAX_CGROUPS];
	struct buffer *buf;
	time_t start_time;
	uint64_t lru_cached;
	uint64_t lru_missing;
};

struct nessdb *db_open(size_t bufferpool_size, const char *basedir, int is_log_recovery);
int db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
int db_exists(struct nessdb *db, struct slice *sk);
int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
void db_remove(struct nessdb *db, struct slice *sk);
char *db_info(struct nessdb *db);
void db_close(struct nessdb *db);

#endif
