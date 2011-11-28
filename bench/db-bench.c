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

/*NOTE:
	How to do
	=========
		$make db-bench
	 	$./db-bench add
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
#include "../engine/db.h"

#define KEYSIZE 	16
#define VALSIZE 	80
#define NUM 		50000
#define R_NUM 		20000
#define REMOVE_NUM	20000
#define BUFFERPOOL	(1024*1024*1024)
#define LINE 		"+----------------------+---------------------------+----------------------------------+-----------------------+\n"
#define LINE1		"--------------------------------------------------------------------------------------------------------------\n"

#include <sys/time.h>

static void start_timer(struct timeval *start)
{
    gettimeofday(start, NULL);
}

static double get_timer(struct timeval *start)
{
    struct timeval end;
    gettimeofday(&end, NULL);
    long seconds = end.tv_sec - start->tv_sec;
    long nseconds = (end.tv_usec - start->tv_usec) * 1000;
    return seconds + (double) nseconds / 1.0e9;
}

static void
fill_random( struct slice *s ) {
	int i;
	for( i = 0; i < s->len; i++ ) {
		s->data[i] = rand() % 0xFF;
	}
}

struct benchmark {
	const char *name;
	uint32_t count;
	uint32_t ok_count;
	double cost;
	uint64_t io_bytes;
	struct nessdb *db;
};
typedef struct benchmark benchmark_t;

/**
 * Runs operation `i` for benchmark `b` and returns total bytes of DB I/O
 */
typedef uint32_t (*benchmark_op_t)(struct benchmark *b, uint32_t i, void* d);


static void
benchmark_reset( benchmark_t *self ) {
	self->count = 0;
	self->ok_count = 0;
	self->cost = 0;
}


static void
benchmark_init( benchmark_t* self, const char *name, struct nessdb* db ) {
	assert( self != NULL );
	assert( db != NULL );

	memset(self, 0, sizeof(benchmark_t));
	self->name = name;
	self->db = db;
}


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
	printf("| %-20s | %.1f%% hit ratio | %.6f sec/op | %d ops/sec | %.1f MiB/sec | %.2f sec runtime |\n"
		,self->name
		,(double)(self->ok_count / (self->count / 100))
		,(double)(self->cost / self->count)
		,(int)(self->count / self->cost)
		,(self->io_bytes / self->cost)
		,self->cost);
	printf(LINE);
}


static uint32_t
bop_null(benchmark_t *b, uint32_t i, void *d) {
	return 0;
}
static void
db_test_null( struct nessdb *db ) {
	benchmark_t b;
	benchmark_init(&b, "Null-Op", db);
	benchmark_run(&b, 100000, bop_null, NULL, "Doing nothing");
	benchmark_report(&b);
}

static uint32_t
bop_write_random_k20_v100(benchmark_t *b, uint32_t i, void *d) {
	char key[20];
	char val[100];
	struct slice sk = {key, sizeof(key)};
	struct slice sv = {val, sizeof(val)};
	fill_random(&sk);
	fill_random(&sv);
	db_add(b->db, &sk, &sv);

	return sk.len + sv.len;
}
static void
db_test_writerand( struct nessdb *db ) {
	benchmark_t b;
	benchmark_init(&b, "Random-Write", db);
	benchmark_run(&b, 100000, bop_write_random_k20_v100, NULL, "writing random 20 byte keys and 100 byte values");
	benchmark_report(&b);
}


static uint32_t
bop_read_random_k20(benchmark_t *b, uint32_t i, void *d) {
	char key[20];
	struct slice sk = {key, sizeof(key)};
	struct slice sv = {NULL, 0};
	fill_random(&sk);
	
	if( db_get(b->db, &sk, &sv) ) {
		free(sv.data);
	}
	return sv.len;
}

static void
db_test_readrand( struct nessdb *db ) {
	benchmark_t b;
	benchmark_init(&b, "Random-Read", db);
	benchmark_run(&b, 100000, bop_read_random_k20, NULL, "reading random non-existant 20 byte keys");
	benchmark_report(&b);
}


static uint32_t
bop_read_sequence_k20(benchmark_t *b, uint32_t i, void *d) {
	char key[20];
	struct slice sk = {key, sizeof(key)};
	struct slice sv = {NULL, 0};
	memset(key, 'X', sizeof(key));
	*(uint32_t*)key = i;

	if( db_get(b->db, &sk, &sv) ) {
		free(sv.data);
	}
	return sv.len;
}
static void
db_test_readseq( struct nessdb *db ) {
	benchmark_t b;
	benchmark_init(&b, "Seq-Read", db);
	benchmark_run(&b, 100000, bop_read_sequence_k20, NULL, "reading non-existant keys sequentially");
	benchmark_report(&b);
}


static uint32_t
bop_remove_sequence_k20(benchmark_t *b, uint32_t i, void *d) {
	char key[20];
	struct slice sk = {key, sizeof(key)};
	memset(key, 'X', sizeof(key));
	*(uint32_t*)key = i;

	db_remove(b->db, &sk);
	return 0;
}
static void
db_test_removeseq( struct nessdb *db ) {
	benchmark_t b;
	benchmark_init(&b, "Remove-Seq", db);
	benchmark_run(&b, 100000, bop_remove_sequence_k20, NULL, "removing non-existant keys sequentially");
	benchmark_report(&b);
}


static void
print_header(void)
{
	printf(LINE1);
	printf("Keys:		%d bytes each\n", KEYSIZE);
	printf("Values:		%d bytes each\n", VALSIZE);
	printf("Entries:	%d\n", NUM);
	//It would be nice to estimate, but it would be better to show the
	//I/O size compared to the on-disk size.
	//printf("IndexSize:	%.1f MB (estimated)\n", _index_size);
	//printf("DataSize:	%.1f MB (estimated)\n", _data_size);
	printf(LINE1);
}


static void
print_environment(void)
{
	time_t now = time(NULL);
	printf("nessDB:		version %s(%s)\n", NESSDB_VERSION, NESSDB_FEATURES);
	printf("Compiler:	%s\n", __VERSION__);
	printf("Date:		%s\n", (char*)ctime(&now));
	
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
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:		%d * %s", num_cpus, cpu_type);
		printf("CPUCache:	%s\n", cache_size);
	}
	printf(LINE);
}


struct nessdb *db_init_test(int show)
{
	return db_open(BUFFERPOOL, getcwd(NULL,0));
}

typedef void (*benchmark_runner_t)( struct nessdb * );
struct benchtype {
	char* name;
	benchmark_runner_t runner;
};

static struct benchtype available_benchmarks[] = {
	{"null", db_test_null},
	{"readrand", db_test_readrand},
	{"readseq", db_test_readseq},
	{"writerand", db_test_writerand},
	{"removeseq", db_test_removeseq},
	{NULL, NULL}	
};

static void
print_usage( char *prog ) {
	fprintf(stderr,
		"Usage: db-bench [options] <benchmark-name>\n"
		"\t-d <dir> Database directory (default: .)\n"
		"\t-n <num> Number of operations to perform (default: 500000)\n"
		"\t-k <num> Key size in bytes (default: 20)\n"
		"\t-v <num> Value size in bytes (default: 100)\n"
		"\t-c <mb>  Cache size in megabytes (default: 4)\n"
		"\n"
		"Benchmarks:\n");
	
	struct benchtype *b = &available_benchmarks[0];
	while( b->name ) {
		fprintf(stderr, "\t%s\n", b->name);
		b++;
	}
	fprintf(stderr, "\n");
}

int main(int argc, char** argv)
{
	bool cant_run = false;
	int opt_num = 500000, opt_key = 20, opt_val = 100, opt_cache = 4;
	char *opt_dir = NULL;
	char* op_name = NULL;
	srand(time(NULL));

	int c;
	while( (c = getopt(argc, argv, "d:n:k:v:c:")) != -1 ) {
		switch( c ) {
		case 'd':
			opt_dir = optarg;
			break;

		case 'n':
			opt_num = atoi(optarg);
			break;

		case 'k':
			opt_key = atoi(optarg);
			break;

		case 'v':
			opt_val = atoi(optarg);
			break;
		
		case 'c':
			opt_cache = atoi(optarg);
			break;

		default:
			fprintf(stderr, "Unknown option -%c\n", c);
			cant_run = true;
			break;
		}
	}

	if( ! cant_run ) {
		if( optind < argc ) {
			op_name = argv[optind];
		}
		else {
			cant_run = true;
		}
	}

	struct benchtype *b = NULL;
	if( ! cant_run ) {
		b = &available_benchmarks[0];
		while( b->name ) {
			if( ! strcmp(b->name, op_name) ) {
				break;
			}
			b++;
		}
		if( b->name == NULL ) {
			cant_run = true;
		}
	}

	if( cant_run ) {
		print_usage(argv[0]);
		return EXIT_FAILURE;
	}

	if( ! opt_dir ) {
		opt_dir = getcwd(NULL,0);
	}
	struct nessdb *db = db_open(opt_cache * 1024 * 1024, opt_dir);
	if( ! db ) {
		errx(EXIT_FAILURE, "Cannot open database in directory '%s' with %dMiB cache", opt_dir, opt_cache);
	}

	print_header();
	print_environment();
	b->runner(db);
	db_close(db);
	return EXIT_SUCCESS;
}
