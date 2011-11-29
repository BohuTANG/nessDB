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
#define LINE 		"+----------------------+-----------------+-----------------+--------------------+---------------+--------------------+\n"
#define LINE1		"+--------------------------------------------------------------------------------------------------------------------+\n"

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
	for( size_t i = 0; i < s->len; i++ ) {
		s->data[i] = rand() % 0xFF;
	}
}

struct benchmark {
	const char *work_dir;
	const char *name;
	size_t entries;
	size_t key_len;
	size_t val_len;
	size_t cache_mb;

	uint32_t count;
	uint32_t ok_count;
	double cost;
	uint64_t io_bytes;

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

static void
benchmark_run(benchmark_t *self, uint32_t count, benchmark_op_t op, void *op_data, char *progress)
{
	uint32_t i;
	struct timeval start;
	uint32_t prog_stop = (count / 200);

	assert( self != NULL );
	assert( op != NULL );
	assert( count > 0 );

	start_timer(&start);
	for (i = 0; i < count; i++) {
		uint32_t io_bytes = op(self, i, op_data);
		self->count++;
		self->ok_count += (io_bytes ? 1 : 0);
		self->io_bytes += io_bytes;
		if( progress && (i % prog_stop == 0) ) {
			fprintf(stderr, "%3d%% -- %70s\r", i / (count / 100), progress);
		}
	}

	self->cost += get_timer(&start);	
}


static void
benchmark_report(benchmark_t *self) {
	printf("| %-20s | %4.1f%% hit ratio | %8.6f sec/op | %10u ops/sec | %5.1f MiB/sec | %6.2f sec runtime |\n"
		,self->name
		,(double)(self->ok_count / (self->count / 100))
		,(double)(self->cost / self->count)
		,(int)(self->count / self->cost)
		,(self->io_bytes / self->cost)
		,self->cost);
	printf(LINE);
}


static uint32_t
bop_null(benchmark_t* b, uint32_t i, void* d) {
	return 0;
}
static void
db_test_null( benchmark_t* b ) {
	benchmark_run(b, 100000, bop_null, NULL, "Doing nothing");
	benchmark_report(b);
}


static uint32_t
bop_write_random(benchmark_t* b, uint32_t i, void* d) {
	char key[b->key_len];
	char val[b->val_len];
	struct slice sk = {key, b->key_len};
	struct slice sv = {val, b->val_len};
	fill_random(&sk);
	fill_random(&sv);
	db_add(b->db, &sk, &sv);

	return sk.len + sv.len;
}
static void
db_test_writerand( benchmark_t *b ) {
	benchmark_run(b, 100000, bop_write_random, NULL, "writing random 20 byte keys and 100 byte values");
	benchmark_report(b);
}


static uint32_t
bop_read_random(benchmark_t* b, uint32_t i, void* d) {
	char key[b->key_len];
	struct slice sk = {key, b->key_len};
	struct slice sv = {NULL, 0};
	fill_random(&sk);
	
	if( db_get(b->db, &sk, &sv) ) {
		free(sv.data);
	}
	return sv.len;
}
static void
db_test_readrand( benchmark_t* b ) {
	benchmark_run(b, 100000, bop_read_random, NULL, "reading random non-existant 20 byte keys");
	benchmark_report(b);
}


static uint32_t
bop_read_sequence(benchmark_t* b, uint32_t i, void* d) {
	char key[b->key_len];
	struct slice sk = {key, b->key_len};
	struct slice sv = {NULL, 0};
	memset(key, 'X', b->key_len);
	*(uint32_t*)key = i;

	if( db_get(b->db, &sk, &sv) ) {
		free(sv.data);
	}
	return sv.len;
}
static void
db_test_readseq( benchmark_t* b ) {
	benchmark_run(b, 100000, bop_read_sequence, NULL, "reading non-existant keys sequentially");
	benchmark_report(b);
}


static uint32_t
bop_remove_sequence(benchmark_t* b, uint32_t i, void* d) {
	char key[b->key_len];
	struct slice sk = {key, b->key_len};
	memset(key, 'X', b->key_len);
	*(uint32_t*)key = i;

	db_remove(b->db, &sk);
	return 0;
}
static void
db_test_removeseq( benchmark_t *b ) {
	benchmark_run(b, 100000, bop_remove_sequence, NULL, "removing non-existant keys sequentially");
	benchmark_report(b);
}

static struct benchmark_controller
available_benchmarks[] = {
	{"null", db_test_null},
	{"readrand", db_test_readrand},
	{"readseq", db_test_readseq},
	{"writerand", db_test_writerand},
	{"removeseq", db_test_removeseq},
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
		&& (b->entries > 0)
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
		"Usage: db-bench [options] <benchmark-name>\n"
		"\t-d <dir> Database directory (default: .)\n"
		"\t-e <num> Number of DB entries (default: 500000)\n"
		"\t-k <num> Key size in bytes (default: 20)\n"
		"\t-v <num> Value size in bytes (default: 100)\n"
		"\t-c <mb>  Cache size in megabytes (default: 4)\n"
		"\n"
		"Benchmarks:\n");
	
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
		bench.name = argv[optind];
	}
	
	if( ! benchmark_validate(&bench) ) {
		print_usage(argv[0]);
		return EXIT_FAILURE;
	}

	printf(LINE1);
	print_environment();
	printf(LINE1);
	printf("  Cache:    %zu MiB\n", bench.cache_mb);
	printf("  Keys:     %zu bytes each\n", bench.key_len);
	printf("  Values:   %zu bytes each\n", bench.val_len);
	printf("  Entries:  %zu\n", bench.entries);
	printf(LINE1);

	struct nessdb *db = db_open(bench.cache_mb * 1024 * 1024, bench.work_dir);
	if( ! db ) {
		errx(EXIT_FAILURE, "Cannot open database");
	}
	srand(time(NULL));
	bench.controller(&bench);
	db_close(db);
	return EXIT_SUCCESS;
}
