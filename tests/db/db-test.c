#include "u.h"
#include "ness.h"
#include "ctest.h"

int KSIZE = 16;
int VSIZE = 100;
int DB_COUNT = 2015102;
int MAX_THREADS = 4;

struct db_extra {
	int start;
	int end;
	struct env *env;
	struct nessdb *db;
};

void _db_insert(void *e)
{
	int i;
	char kbuf[KSIZE];
	struct db_extra *extra = (struct db_extra*)e;
	struct nessdb *db = extra->db;
	struct random *rnd = rnd_new();
	for (i = extra->start; i < extra->end; i++) {
		char *vbuf = rnd_str(rnd, VSIZE);

		snprintf(kbuf, KSIZE, "%016d", i);
		ness_db_set(db, kbuf, KSIZE, vbuf, VSIZE);
	}
	xfree(extra);
}

void _db_benchmark(int thds)
{
	int i = 0;
	long long cost;
	struct db_extra *extra;
	struct kibbutz *ktz = kibbutz_new(thds + 1);
	struct timespec ts1, ts2;

	char basedir[] = "./dbbench/";
	char dbname[] = "test.db";
	struct env *env = ness_env_open(basedir, -1);
	struct nessdb *db = ness_db_open(env, dbname);
	if (!db) {
		fprintf(stderr, "open db error, see ness.event for details\n");
		return;
	}

	ness_gettime(&ts1);
	for (i = 0; i < thds; i++) {
		extra = xcalloc(1, sizeof(*extra));
		extra->env = env;
		extra->db = db;
		extra->start = i * ((DB_COUNT) / thds);
		extra->end = (i + 1) * ((DB_COUNT) / thds);
		kibbutz_enq(ktz, _db_insert, extra);
	}
	kibbutz_free(ktz);
	ness_gettime(&ts2);
	cost = ness_time_diff_ms(ts1, ts2);
	if (thds == 1)
		printf("\n");
	printf("\tthds=%d, cost=%lldms, %lld/sec\n", thds, cost, DB_COUNT * 1000 / cost);

	ness_db_close(db);
	ness_env_close(env);
}

CTEST(db, multi_threads_insert)
{
	_db_benchmark(MAX_THREADS);
}
