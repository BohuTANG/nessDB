/*
 * Please NOTICE DB limits:
 * https://github.com/shuttler/nessDB/wiki/nessDB-limits
 */
#ifndef __nessDB_H
#define __nessDB_H

struct slice {
	char *data;
	int len;
};

struct iter {
	int c;
	int idx;
	int valid;
	struct slice *key;
	struct slice *value;
	struct ness_kv *kvs;
};

#ifdef __cplusplus
	extern "C" {
#endif

	struct nessdb *db_open(const char *basedir);
	int db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
	int db_exists(struct nessdb *db, struct slice *sk);
	int db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
	void db_stats(struct nessdb *db, struct slice *infos);
	void db_remove(struct nessdb *db, struct slice *sk);
	void db_close(struct nessdb *db);
	void db_free_data(void *data);

	/* 
	 * Range: [start, end)
	 */
	struct iter *db_scan(struct nessdb *db, 
				struct slice *start, struct slice *end, 
				int limit);
	void db_iter_next(struct iter *it);

#ifdef __cplusplus
	}
#endif

#endif
