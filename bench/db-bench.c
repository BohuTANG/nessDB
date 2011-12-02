/* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of nessDB nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* HarryR:
 * The series of benchmarks we need to provide should be comparable to the
 * LevelDB benchmarks, but flexible enough to apply to a load of different
 * database systems.
 *
 * The benchmarks should be used in-place of a test suite and should represent
 * many different use cases and scenarios.
 *
 * Reference at: http://code.google.com/p/leveldb/source/browse/db/db_bench.cc
 *
 * TODO: output CSV of results
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <err.h>
#include <sys/time.h>

#include "../engine/db.h"

// Hand crafted so the output looks nice
#define LINE 		"+----------------------+-----------+-----------------+--------------------+---------------+--------------------+\n"
#define LINE1		"+--------------------------------------------------------------------------------------------------------------+\n"

static void
start_timer(struct timeval *start)
{
    gettimeofday(start, NULL);
}

static double
get_timer(struct timeval *start)
{
    struct timeval end;
    gettimeofday(&end, NULL);
    long seconds = end.tv_sec - start->tv_sec;
    long nseconds = (end.tv_usec - start->tv_usec) * 1000;
    return seconds + (double) nseconds / 1.0e9;
}

static void
fill_random( struct slice *s ) {
	size_t i;
	for( i = 0; i < s->len; i++ ) {
		s->data[i] = rand() % 0xFF;
	}
}

struct benchmark {
	const char *work_dir;
	char *name;
	size_t entries;
	size_t key_len;
	size_t val_len;
	size_t cache_mb;

	uint32_t count;
	uint32_t ok_count;
	uint32_t cost;
	uint64_t io_bytes;

	struct timeval start;
	struct nessdb *db;
	void (*controller)( struct benchmark* );
};
typedef struct benchmark benchmark_t;

struct benchmark_controller {
	char* name;
	void (*runner)( struct benchmark* );
};

/**
 * Runs operation `i` for benchmark `b` and returns total bytes of DB I/O
 */
typedef uint32_t (*benchmark_op_t)(struct benchmark *b, uint32_t i, void* d);

#define cycle32(i) ((i >> 1) ^ (-(i & 1u) & 0xD0000001u))

static void
benchmark_reset( benchmark_t *self ) {
	self->count = 0;
	self->ok_count = 0;
	self->io_bytes = 0;
	self->cost = 0;
}

static size_t
benchmark_bps(benchmark_t *self) {
	return (self->io_bytes / (get_timer(&self->start) + 1));
}


static void
benchmark_op(benchmark_t *self, uint32_t count, benchmark_op_t op, void *op_data, char *progress)
{
	uint32_t i;
	uint32_t prog_stop = (count / 50);

	assert( self != NULL );
	assert( op != NULL );
	assert( count > 0 );

	for (i = 0; i < count; i++) {
		uint32_t io_bytes = op(self, i, op_data);
		self->count++;
		self->ok_count += (io_bytes ? 1 : 0);
		self->io_bytes += io_bytes;
		if( progress && (i % (prog_stop + 1)) == 0) {
			fprintf(stderr, "%3zu%% @ %5.1fmb/s -- %-50s\r", self->count / (self->entries / 100), benchmark_bps(self) / 1024.0 / 1024.0, progress);
		}
	}

	self->cost += get_timer(&self->start);	
}


static void
benchmark_report(benchmark_t *self) {
	if( self->cost == 0 ) return;

	printf("| %-20s | %4.1f%% OK | %8.6f sec/op | %10u ops/sec | %5.1f MiB/sec | %d sec runtime |\n"
		,self->name
		,(double)(self->ok_count / (self->count / 100.0))
		,(double)(self->cost / self->count)
		,(int)(self->count / self->cost)
		,benchmark_bps(self) / 1024.0 / 1024.0
		,self->cost);
	printf(LINE);
}


static void
benchmark_run(benchmark_t *self) {
	assert( self->db == NULL );
	self->db = db_open(self->cache_mb * 1024 * 1024, self->work_dir);
	if( ! self->db ) {
		errx(EXIT_FAILURE, "Cannot open database");
	}
	srand(time(NULL));

	printf(LINE);
	start_timer(&self->start);
	self->controller(self);
	db_flush(self->db);
	benchmark_report(self);
	db_close(self->db);
}


static uint32_t
bop_null(benchmark_t* b, uint32_t i, void* d) {
	(void)b; (void)i; (void)d;
	return 0;
}
static void
db_test_null( benchmark_t* b ) {
	benchmark_op(b, b->entries, bop_null, NULL, "Doing nothing");
}

static uint32_t
bop_read_thrash(benchmark_t* b, uint32_t i, void* d) {
	(void)i; (void)d;
	char key[b->key_len];
	struct slice sk = {key, b->key_len, 0};
	struct slice sv = {NULL, 0, 0};
	fill_random(&sk);
	if( db_get(b->db, &sk, &sv) ) {
		free(sv.data);
	}
	return sk.len + sv.len;
}

static uint32_t
bop_write_thrash(benchmark_t* b, uint32_t i, void* d) {
	(void)i; (void)d;
	char key[b->key_len];
	char val[b->val_len];
	struct slice sk = {key, b->key_len, 0};
	struct slice sv = {val, b->val_len, 0};
	fill_random(&sk);
	fill_random(&sv);
	db_add(b->db, &sk, &sv);
	return sk.len + sv.len;
}

static uint32_t
bop_write_random(benchmark_t* b, uint32_t i, void* d) {
	(void)i; (void)d;
	char key[b->key_len];
	char val[b->val_len];
	struct slice sk = {key, b->key_len, 0};
	struct slice sv = {val, b->val_len, 0};

	i = cycle32((uint32_t)(rand() % b->entries));
	snprintf(key, b->key_len, "%X", i);
	snprintf(val, b->val_len, "V%XA%XL%XU%XE%X", i,  i, i, i, i);

	/*
	fill_random(&sk);
	fill_random(&sv);
	 */
	db_add(b->db, &sk, &sv);

	return sk.len + sv.len;
}
static void
db_test_writerand( benchmark_t *b ) {
	char desc[400];
	snprintf(desc, sizeof(desc), "writing random %zub keys and %zub values", b->key_len, b->val_len);
	benchmark_op(b, b->entries, bop_write_random, NULL, desc);
	db_flush(b->db);
}


static uint32_t
bop_read_random(benchmark_t* b, uint32_t i, void* d) {
	(void)i; (void)d;
	char key[b->key_len];
	char val[b->val_len];
	struct slice sk = {key, b->key_len, 0};
	struct slice sv = {NULL, 0, 0};
	memset(key, 'X', b->key_len);
	memset(val, 'X', b->val_len);

	i = cycle32((uint32_t)(rand() % b->entries));
	snprintf(key, b->key_len, "%X", i);
	snprintf(val, b->val_len, "V%XA%XL%XU%XE%X", i,  i, i, i, i);

	if( db_get(b->db, &sk, &sv) ) {
		assert( sk.data != sv.data );
		if( sv.len != b->val_len || strncmp(sv.data, val, b->val_len) != 0 ) {
			sv.len = 0;
		}
		free(sv.data);
	}
	return sk.len + sv.len;
}
static void
db_test_readrand( benchmark_t* b ) {
	benchmark_op(b, b->entries / 2, bop_read_random, NULL, "reading random keys in 32bit space");
	benchmark_op(b, b->entries / 2, bop_write_random, NULL, "writing random keys in 32bit space");
	db_flush(b->db);
	benchmark_op(b, b->entries / 2, bop_read_random, NULL, "reading random keys in 32bit space");
}

static uint32_t
bop_write_sequence(benchmark_t* b, uint32_t i, void* d) {
	(void)d;
	char key[b->key_len];
	char val[b->val_len];
	struct slice sk = {key, b->key_len, 0};
	struct slice sv = {val, b->val_len, 0};
	memset(key, 'X', b->key_len);
	memset(val, 'X', b->val_len);
	snprintf(key, b->key_len, "%X", i);
	snprintf(val, b->val_len, "V%XA%XL%XU%XE%X", i,  i, i, i, i);

	if( ! db_add(b->db, &sk, &sv) ) {
		return 0;
	}
	return sk.len + sk.len;
}
static void
db_test_writeseq( benchmark_t *b ) {
	benchmark_op(b,  b->entries, bop_write_sequence, NULL, "writing keys sequentially");
	db_flush(b->db);
}

static uint32_t
bop_read_sequence(benchmark_t* b, uint32_t i, void* d) {
	(void)i; (void)d;
	char key[b->key_len];
	char val[b->val_len];
	struct slice sk = {key, b->key_len, 0};
	struct slice sv = {NULL, 0, 0};
	memset(key, 'X', b->key_len);
	memset(val, 'X', b->val_len);
	snprintf(key, b->key_len, "%X", i);
	snprintf(val, b->val_len, "V%XA%XL%XU%XE%X", i, i, i, i, i);

	if( db_get(b->db, &sk, &sv) ) {
		assert( sk.data != sv.data );
		if( sv.len != b->val_len || strncmp(sv.data, val, b->val_len) != 0 ) {
			sv.len = 0;
		}
		free(sv.data);
	}
	return sk.len + sv.len;
}
static void
db_test_readseq( benchmark_t* b ) {
	benchmark_op(b, b->entries / 2, bop_write_sequence, NULL, "filling test data");
	benchmark_op(b, b->entries / 2, bop_read_sequence, NULL, "reading keys sequentially");
}


static uint32_t
bop_remove_sequence(benchmark_t* b, uint32_t i, void* d) {
	(void)d;
	char key[b->key_len];
	struct slice sk = {key, b->key_len, 0};
	memset(key, 'X', b->key_len);
	snprintf(key, b->key_len, "%X", i);
	db_remove(b->db, &sk);
	return sk.len;
}
static void
db_test_removeseq( benchmark_t *b ) {
	benchmark_op(b, 100000, bop_remove_sequence, NULL, "removing non-existant keys sequentially");
}

static void
run_test_rwmix( benchmark_t *b, size_t entries ) {
	size_t run_count, i, op;

	run_count = entries / 100;
	for( i = 0; i < 100; i++ ) {
		op = cycle32(i) % 7;
		if( ! op-- ) benchmark_op(b, run_count, bop_remove_sequence, NULL, "Remove sequential");
		if( ! op-- ) benchmark_op(b, run_count, bop_write_random, NULL, "Write random");
		if( ! op-- ) benchmark_op(b, run_count, bop_write_sequence, NULL, "Write Sequence");
		if( ! op-- ) benchmark_op(b, run_count, bop_write_thrash, NULL, "Write Thrash");
		if( ! op-- ) benchmark_op(b, run_count, bop_read_sequence, NULL, "Read Sequence");
		if( ! op-- ) benchmark_op(b, run_count, bop_read_thrash, NULL, "Read Thrash");
		else benchmark_op(b, run_count, bop_read_random, NULL, "Read Random");

		if( i % 3 == 0 ) {
			db_flush(b->db);
			fflush(stderr);
			benchmark_report(b);
		}
	}
}

static void
db_test_rwmix( benchmark_t *b ) {
	run_test_rwmix(b, b->entries);
}

static struct benchmark_controller
available_benchmarks[] = {
	{"null", db_test_null},
	{"readrandom", db_test_readrand},
	{"readseq", db_test_readseq},
	{"writerand", db_test_writerand},
	{"writeseq", db_test_writeseq},
	{"removeseq", db_test_removeseq},
	{"rwmix", db_test_rwmix},
	{NULL, NULL}	
};

static bool
benchmark_validate( benchmark_t *b ) {
	struct benchmark_controller* r = &available_benchmarks[0];
	if( b->name == NULL ) {
		warnx("No benchmark name specified");
		return false;
	}
	while( r->name ) {
		if( ! strcmp(b->name, r->name) ) {
			b->controller = r->runner;
			break;
		}
		r++;
	}
	if( r->name == NULL ) {
		warnx("Unknown benchmark name '%s'", b->name);
		return false;
	}

	return (b->name != NULL)
		&& (b->work_dir != NULL)
		&& (b->entries > 100)
		&& (b->key_len > 0)
		&& (b->val_len > 0);
}

static void
print_environment(void)
{
	time_t now = time(NULL);
	
	printf("  nessDB:	version %s (%s)\n", NESSDB_VERSION, NESSDB_FEATURES);
	printf("  Compiler:	%s\n", __VERSION__);
	printf("  Date:		%s", (char*)ctime(&now));
	
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
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strncpy(cpu_type, val, strlen(val) - 1);
			}
			else if(strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 2);	
		}

		fclose(cpuinfo);
		printf("  CPU:		%d * %s\n", num_cpus, cpu_type);
		printf("  CPUCache:	%s\n", cache_size);
	}
}

static void
print_usage( char *prog ) {
	fprintf(stderr,
		"Usage: %s [options] <benchmark-name>\n"
		"\t-d <dir> Database directory (default: .)\n"
		"\t-e <num> Number of DB entries (default: 500000)\n"
		"\t-k <num> Key size in bytes (default: 20)\n"
		"\t-v <num> Value size in bytes (default: 100)\n"
		"\t-c <mb>  Cache size in megabytes (default: 4)\n"
		"\n"
		"Benchmarks:\n", prog);
	
	struct benchmark_controller *b = &available_benchmarks[0];
	while( b->name ) {
		fprintf(stderr, "\t%s\n", b->name);
		b++;
	}
	fprintf(stderr, "\n");
}

int
main(int argc, char** argv)
{
	int c;
	benchmark_t bench = {
		.work_dir = NULL,
		.name = NULL,
		.entries = 500000,
		.key_len = 20,
		.val_len = 100,
		.cache_mb = 4
	};
	benchmark_reset(&bench);

	while( (c = getopt(argc, argv, "d:e:k:v:c:")) != -1 ) {
		switch( c ) {
		case 'd':
			bench.work_dir = optarg;
			break;

		case 'e':
			bench.entries = atoi(optarg);
			break;

		case 'k':
			bench.key_len = atoi(optarg);
			break;

		case 'v':
			bench.val_len = atoi(optarg);
			break;
		
		case 'c':
			bench.cache_mb = atoi(optarg);
			break;

		default:
			fprintf(stderr, "Unknown option -%c\n", c);
			break;
		}
	}

	if( ! bench.work_dir ) {
		bench.work_dir = getcwd(NULL, 0);
	}

	if( optind < argc ) {
		bench.name = malloc(strlen(argv[optind]) + 1);
		strcpy(bench.name, argv[optind]);
	}
	
	if( ! benchmark_validate(&bench) ) {
		print_usage(argv[0]);
		free(bench.name);
		return EXIT_FAILURE;
	}

	printf(LINE1);
	print_environment();
	printf(LINE1);
	printf("  Cache:    %zu MiB\n", bench.cache_mb);
	printf("  Keys:     %zu bytes each\n", bench.key_len);
	printf("  Values:   %zu bytes each\n", bench.val_len);
	printf("  Entries:  %zu\n", bench.entries);

	char *info = db_info(bench.db);
	printf("\nDB Info:\n%s", info);
	free(info);

	benchmark_run(&bench);
	free(bench.name);
	return EXIT_SUCCESS;
}
