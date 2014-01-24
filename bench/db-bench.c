#include "db.h"
#include "posix.h"
#include "random.h"

#define KEY_SIZE (16)
#define VAL_SIZE (100)

void do_bench(struct nessdb *db, struct random *rnd, uint32_t loop)
{
	uint32_t i;
	int done = 0;;
	int next_report = 100;
	char kbuf[KEY_SIZE];

	for (i = 0; i < loop; i++) {
		uint32_t krnd = rnd_next(rnd);
		snprintf(kbuf, KEY_SIZE, "%016d", krnd);
		char *vbuf = rnd_str(rnd, VAL_SIZE);

		struct msg k = {.data = kbuf, .size = KEY_SIZE};
		struct msg v = {.data = vbuf, .size = VAL_SIZE};
		db_set(db, &k, &v);

		done++;

		if (done >= next_report) {
			if      (next_report < 1000)   next_report += 100;
			else if (next_report < 5000)   next_report += 500;
			else if (next_report < 10000)  next_report += 1000;
			else if (next_report < 50000)  next_report += 5000;
			else if (next_report < 100000) next_report += 10000;
			else if (next_report < 500000) next_report += 50000;
			else                            next_report += 100000;
			fprintf(stderr,
					"random write finished %d ops%30s\r",
					done,
					"");

			fflush(stderr);
		}
	}
}

int main(int argc, char *argv[])
{

	int loop;
	struct nessdb *db;
	char basedir[] = "./dbbench/";
	struct timespec start;
	struct timespec end;
	struct random *rnd;

	if (argc != 2) {
		printf("./db-bench [count]\n");
		return 0;
	}
	loop = atoi(argv[1]);
	rnd = rnd_new();
	db = db_open(basedir);

	gettime(&start);
	do_bench(db, rnd, loop);
	gettime(&end);

	uint64_t bytes = loop * (KEY_SIZE + VAL_SIZE);
	long long cost_ms = time_diff_ms(start, end) + 1;
	printf("--------loop:%d, cost:%d(ms), %.f ops/sec, %6.1f MB/sec\n",
			loop,
			(int)cost_ms,
			(double)(loop/cost_ms)*1000,
			(double)((bytes/cost_ms/1048576.0) * 1000));


	db_close(db);
	rnd_free(rnd);

	return 1;
}
