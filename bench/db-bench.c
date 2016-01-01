#include "u.h"
#include "ness.h"
#include "random.h"

#define V "3.0.0"
#define LINE1 "------------------------------------------------------------\n"

#define KEY_SIZE (20)
#define VAL_SIZE (100)

void *e;
void *db;
struct random *rnd;
static uint64_t FLAGS_num = 1000000;
static uint64_t FLAGS_cache_size = (1073741824); // 1GB
static const char* FLAGS_benchmarks = "fillrandom";
static int FLAGS_method = 1; //snappy

void _print_warnings()
{
	if (FLAGS_method != 0) {
		switch (FLAGS_method) {
		case 1:
			fprintf(stdout, "Compression:Snappy\n");
			break;
		default:
			break;
		}
	} else
		fprintf(stdout,
		        "WARNING: Snappy compression is disabled\n");
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
	fprintf(stdout,
	        "WARNING: optimization is disabled: benchmarks unnecessarily slow\n"
	       );
#endif

#ifdef ASSERT
	fprintf(stdout,
	        "WARNING: assertions are enabled; benchmarks unnecessarily slow\n");
#endif
}

void _print_environment()
{
	time_t now = time(NULL);
	printf("nessDB:     version %s\n", V);
	printf("Date:       %s", (char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep - 1 - line);
			strncpy(val, sep + 1, strlen(sep) - 1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			} else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);
		}

		fclose(cpuinfo);
		printf("CPU:        %d * %s", num_cpus, cpu_type);
		printf("CPUCache:   %s", cache_size);
	}
}

void _print_header()
{
	_print_environment();
	double raw_size = (double)((KEY_SIZE + VAL_SIZE) * FLAGS_num) / 1048576.0;

	printf("Keys:       %d bytes each\n", KEY_SIZE);
	printf("Values:     %d bytes each\n", VAL_SIZE);
	printf("Entries:    %" PRIu64 "\n", FLAGS_num);
	printf("RawSize:    %.1f MB (estimated)\n", raw_size);
	_print_warnings();
	fprintf(stdout, LINE1);
}

void dbwrite(char *name, int random)
{
	uint32_t i;
	int done = 0;;
	int next_report = 100;
	char kbuf[KEY_SIZE];
	histogram h;
	histogram_init(&h);

	for (i = 0; i < FLAGS_num; i++) {
		char *vbuf;
		int key = random ? rnd_next(rnd) % FLAGS_num : i;

		memset(kbuf, 0, KEY_SIZE);
		snprintf(kbuf, KEY_SIZE, "%016d", key);
		vbuf = rnd_str(rnd, VAL_SIZE);

		double t0 = histogram_time();
		if (ness_db_set(db, kbuf, strlen(kbuf), vbuf, VAL_SIZE) != NESS_OK) {
			fprintf(stderr, " set error\n");
		}
		double t1 = histogram_time();
		double tb = t1 - t0;
		histogram_add(&h, tb);

		done++;
		if (done >= next_report) {
			if (next_report < 1000)   next_report += 100;
			else if (next_report < 5000)   next_report += 500;
			else if (next_report < 10000)  next_report += 1000;
			else if (next_report < 50000)  next_report += 5000;
			else if (next_report < 100000) next_report += 10000;
			else if (next_report < 500000) next_report += 50000;
			else                            next_report += 100000;
			fprintf(stderr,
			        "%s finished %d ops%30s\r",
			        name,
			        done,
			        "");
			fflush(stderr);
		}
	}
	histogram_print(&h);
}

void writeseq()
{
	char *name = "seq write";
	struct timespec a, b;

	ness_gettime(&a);
	dbwrite(name, 0);
	ness_gettime(&b);
}

void writerandom()
{
	char *name = "random write";
	struct timespec a, b;

	ness_gettime(&a);
	dbwrite(name, 1);
	ness_gettime(&b);
}

int dbopen()
{
	char basedir[] = "./dbbench/";
	char dbname[] = "test.db";

	e = ness_env_open(basedir, -1);
	db = ness_db_open(e, dbname);
	if (!db) {
		fprintf(stderr, "open db error, see ness.event for details\n");
		return 0;
	}

	if (!ness_env_set_cache_size(e, FLAGS_cache_size)) {
		fprintf(stderr, "set cache size error, see ness.event for details\n");
		return 0;
	}

	if (!ness_db_change_compress_method(db, FLAGS_method)) {
		fprintf(stderr, "set compress method error, see ness.event for details\n");
		return 0;
	}

	return 1;
}

int run()
{
	int ret = 1;
	const char* benchmarks = FLAGS_benchmarks;

	_print_header();

	if (strncmp("fillseq", benchmarks, 7) == 0) {
		writeseq();
	} else if (strncmp("fillrandom", benchmarks, 10) == 0) {
		writerandom();
	} else {
		fprintf(stderr, "unknown benchmark '%s'\n", benchmarks);
		ret = 0;
	}

	return ret;
}

int main(int argc, char *argv[])
{
	int i;

	for (i = 1; i < argc; i++) {
		long n;
		char junk;

		if (strncmp(argv[i], "--benchmarks=", 13) == 0) {
			FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
		} else if (sscanf(argv[i], "--cache_size=%ld%c", &n, &junk) == 1) {
			FLAGS_cache_size = n;
		} else if (sscanf(argv[i], "--num=%ld%c", &n, &junk) == 1) {
			FLAGS_num = n;

		} else if (strncmp(argv[i], "--compress=", 11) == 0) {
			if (strcmp(argv[i] + strlen("--compress="), "snappy") == 0)
				FLAGS_method = 1;
			else if (strcmp(argv[i] + strlen("--compress="), "no") == 0)
				FLAGS_method = 0;
		} else {
			fprintf(stderr, "invalid flag %s\n", argv[i]);
			exit(1);
		}
	}

	srand(time(NULL));
	rnd = rnd_new();
	if (!dbopen())
		exit(1);

	if (!run())
		exit(1);

	rnd_free(rnd);
	ness_db_close(db);
	ness_env_close(e);

	return 1;
}
