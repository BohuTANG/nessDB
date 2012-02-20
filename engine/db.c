 /*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */
 
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "llru.h"
#include "db.h"
#include "debug.h"

#define DB "ness"
#define LIST_SIZE	(5000000)


struct nessdb *db_open(size_t bufferpool_size, const char *basedir, int tolog)
{
	struct nessdb *db;
	 	
	db = malloc(sizeof(struct nessdb));
	db->idx = index_new(basedir, DB, LIST_SIZE, tolog);
	db->lru = llru_new(bufferpool_size);
	db->buf = buffer_new(1024);
	db->start_time = time(NULL);

	return db;
}


int db_add(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	llru_remove(db->lru, sk);
	return index_add(db->idx, sk, sv);
}

int db_get(struct nessdb *db, struct slice *sk, struct slice *sv)
{
	int ret = 0;
	char *data;
	struct slice *sv_l;

	sv_l = llru_get(db->lru, sk);

	if (sv_l) {
		data = malloc(sv_l->len);
		memcpy(data, sv_l->data, sv_l->len);
		sv->len = sv_l->len;
		sv->data = data;
		ret = 1;
	} else {
		ret = index_get(db->idx, sk, sv);
		if (ret == 1) {
			llru_set(db->lru, sk, sv);
		}

	}

	return ret;
}

int db_exists(struct nessdb *db, struct slice *sk)
{
	struct slice sv;
	int ret = index_get(db->idx, sk, &sv);
	if(ret) {
		free(sv.data);
		return 1;
	}
	return 0;
}

void db_remove(struct nessdb *db, struct slice *sk)
{
	llru_remove(db->lru, sk);
	index_add(db->idx, sk, NULL);
}

char *db_info(struct nessdb *db)
{
	time_t uptime = time(NULL) - db->start_time;
	buffer_clear(db->buf);
	buffer_scatf(db->buf, 
			"# Server\r\n"
			"gcc_version:%d.%d.%d\r\n"
			"process_id:%ld\r\n"
			"uptime_in_seconds:%ld\r\n"
			"uptime_in_days:%ld\r\n\r\n"

			"# Stats\r\n"
			"total_lru_count:%d\r\n"
			"total_lru_hot_count:%d\r\n"
			"total_lru_cold_count:%d\r\n"
			"total_lru_memory_usage:%d(MB)\r\n"
			"total_lru_hot_memory_usage:%d(MB)\r\n"
			"total_lru_cold_meomry_usage:%d(MB)\r\n"
			"max_allow_lru_memory_usage:%d(MB)\r\n\r\n"
			"total_memtable_count:%d\r\n"
			"total_background_merge_count:%d\r\n",
#ifdef __GNUC__
			__GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
			0,0,0,
#endif
			(long) getpid(),
			uptime,
			uptime/(3600*24),

			(db->lru->level_old.count + db->lru->level_new.count),
			db->lru->level_new.count,
			db->lru->level_old.count,
			(db->lru->level_new.used_size + db->lru->level_old.used_size) / (1024 * 1024),
			db->lru->level_new.used_size / (1024 * 1024),
			db->lru->level_old.used_size / (1024 * 1024),
			(db->lru->level_old.allow_size + db->lru->level_new.allow_size) / (1024 * 1024),

			db->idx->list->count,
			db->idx->bg_merge_count);

	return buffer_detach(db->buf);
}

void db_close(struct nessdb *db)
{
	llru_free(db->lru);
	index_free(db->idx);
	buffer_free(db->buf);
	free(db);
}
