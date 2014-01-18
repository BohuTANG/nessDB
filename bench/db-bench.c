#include "db.h"
#include "posix.h"
#include "skiplist.h"

#define KEY_SIZE (16)
#define VAL_SIZE (100)

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

int main(int argc, char *argv[])
{
	int i;
	int done = 0;;
	int next_report = 100;

	int loop;
	struct nessdb *db;
	char basedir[] = "./dbbench/";
	struct timespec start;
	struct timespec end;

	if (argc != 2) {
		printf("./db-bench [count]\n");
		return 0;
	}
	loop = atoi(argv[1]);

	db = db_open(basedir);

	gettime(&start);
	char kbuf[KEY_SIZE];
	char vbuf[VAL_SIZE];
	_random_key(vbuf, VAL_SIZE);
	for (i = 0; i < loop; i++) {
		_random_key(kbuf, KEY_SIZE);
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
	gettime(&end);

	long long cost_ms = time_diff_ms(start, end) + 1;
	printf("--------loop:%d, cost:%d(ms), %.f ops/sec\n",
			loop,
			(int)cost_ms,
			(double)(loop/cost_ms)*1000);


	db_close(db);

	return 1;
}
