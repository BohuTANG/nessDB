#ifndef __nessDB_H
#define __nessDB_H

struct slice {
	char *data;
	int len;
};

struct ness_kv {
	struct slice sk;
	struct slice sv;
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
	struct ness_kv *db_scan(struct nessdb *db, 
				struct slice *start, struct slice *end, 
				int limit, int *c);

#ifdef __cplusplus
	}
#endif

#endif
