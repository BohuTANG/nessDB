 /*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */
 
#include <stdlib.h>
#include <string.h>

#include "index.h"
#include "buffer.h"
#include "lru.h"
#include "util.h"
#include "config.h"
#include "db.h"
#include "debug.h"

struct nessdb *db_open(const char *basedir, uint64_t buffer, int is_log_recovery)
{
	char buff_dir[FILE_PATH_SIZE];
	struct nessdb *db;

	db = (struct nessdb*)malloc(sizeof(struct nessdb));
	if (!db)
		__PANIC("malloc nessdb NULL when db_open, abort...");

	db->buf = buffer_new(LOG_BUFFER_SIZE);
	db->start_time = time(NULL);

	memset(buff_dir, 0, FILE_PATH_SIZE);
	snprintf(buff_dir, FILE_PATH_SIZE, "%s/ndbs", basedir);
	db->idx = index_new(buff_dir, MTBL_MAX_COUNT, is_log_recovery); 

	db->lru = lru_new(buffer);

	return db;
}

int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	lru_remove(db->lru, sk);
	return index_add(db->idx, sk, sv);
}

int db_get(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	int ret = 0;

	ret = index_bloom_get(db->idx, sk);
	if (ret) {
		ret = lru_get(db->lru, sk, sv);
		if (ret)
			return 1;
		else {
			ret = index_get(db->idx, sk, sv);
			if (ret)
				lru_set(db->lru, sk, sv);
		}
	}

	return ret;
}

int db_exists(struct nessdb *db, struct slice *sk)
{
	int ret = 0;
	struct slice sv;

	ret = index_bloom_get(db->idx, sk);
	if (ret) {

		int ret = lru_get(db->lru, sk, &sv);
		if (ret) {
			free(sv.data);
			return 1;
		}

		ret = index_get(db->idx, sk, &sv);
		if (ret == 1) {
			free(sv.data);
			return 1;
		}
	}

	return 0;
}

void db_remove(struct nessdb *db, struct slice *sk)
{
	lru_remove(db->lru, sk);
	index_add(db->idx, sk, NULL);
}

char *db_info(struct nessdb *db)
{
	int arch_bits = (sizeof(long) == 8) ? 64 : 32;
	time_t uptime = time(NULL) - db->start_time;
	int upday = uptime / (3600 * 24);

	long long max_merge_time = db->idx->max_merge_time;
	int total_memtable_count = db->idx->list->count;
	uint64_t total_count = index_allcount(db->idx);
	int total_bg_merge_count = db->idx->bg_merge_count;

	buffer_clear(db->buf);
	buffer_scatf(db->buf, 
			"# Server\r\n"
			"nessDB_version:%s\r\n"
			"arch_bits:%d\r\n"
			"gcc_version:%d.%d.%d\r\n"
			"process_id:%ld\r\n"
			"uptime_in_seconds:%d\r\n"
			"uptime_in_days:%d\r\n"
			"max_merge_time_sec:%lu\r\n\r\n"

			"# Stats\r\n"
			"total_memtable_count:%d\r\n"
			"total_count(in sst):%llu\r\n"
			"total_bg_merge_count:%d\r\n\r\n"
		,
			DB_VERSION,
			arch_bits,
#ifdef __GNUC__
			__GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
			0,0,0,
#endif
			(long)getpid(),
			uptime,
			upday,
			max_merge_time,

			total_memtable_count, 
			total_count,
			total_bg_merge_count);

	return buffer_detach(db->buf);
}

void db_close(struct nessdb *db)
{
	index_free(db->idx);
	buffer_free(db->buf);
	lru_free(db->lru);
	free(db);
}
