/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 */

/*NOTE:
	How to do
	=========
		$make db-bench
	 	$./db-bench <op: write | read> <count>
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "../engine/debug.h"
#include "../engine/db.h"

#define DATAS ("ndbs")
#define KSIZE (16)
#define VSIZE (80)
#define V "2.0"
#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"

long long get_ustime_sec(void)
{
	struct timeval tv;
	long long ust;

	gettimeofday(&tv, NULL);
	ust = ((long long)tv.tv_sec)*1000000;
	ust += tv.tv_usec;
	return ust / 1000000;
}

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n", 
			KSIZE);
	printf("Values: \t%d bytes each\n", 
			VSIZE);
	printf("Entries:\t%d\n", 
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("nessDB:\t\tversion %s(LSM-Tree storage engine)\n", 
			V);

	printf("Date:\t\t%s", 
			(char*)ctime(&now));

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
		printf("CPU:\t\t%d * %s", 
				num_cpus, 
				cpu_type);

		printf("CPUCache:\t%s\n", 
				cache_size);
	}
}

void _write_test(long int count)
{
	int i;
	double cost;
	long long start,end;
	struct slice sk, sv, stats;
	struct nessdb *db;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		//_random_key(key, KSIZE);
		snprintf(key, KSIZE, "key-%d", i);
		snprintf(val, VSIZE, "val-%d", i);

		sk.len = KSIZE;
		sk.data = key;
		sv.len = VSIZE;
		sv.data = val;

		db_add(db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	stats.len = 1024;
	stats.data = sbuf;
	db_stats(db, &stats);
	printf("\r\n%s", stats.data);

	db_close(db);

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _writeone_test(char *k, char *v)
{
	struct slice sk, sv;
	struct nessdb *db;

	sk.len = strlen(k);
	sk.data = k;
	sv.len = strlen(v);
	sv.data = v;

	db = db_open(DATAS);
	db_add(db, &sk, &sv);

	db_close(db);
}

void _read_test(long int count)
{
	int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	struct slice sk;
	struct slice sv;
	struct nessdb *db;
	char key[KSIZE + 1];

	db = db_open(DATAS);
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want test random write, using flollowing */
		//_random_key(key, KSIZE);
		snprintf(key, KSIZE, "key-%d", i);
		sk.len = KSIZE;
		sk.data = key;
		ret = db_get(db, &sk, &sv);
		if (ret) {
			db_free_data(sv.data);
			found++;
		} else
			__INFO("not found key#%s", 
					sk.data);

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}

void _readone_test(char *key)
{
	int ret;
	struct slice sk;
	struct slice sv;
	struct nessdb *db;
	char k[KSIZE + 1];
	int len = strlen(key);

	memset(k, 0, KSIZE + 1);
	memcpy(k, key, len);

	db = db_open(DATAS);
	sk.len = (KSIZE + 1);
	sk.data = k;

	ret = db_get(db, &sk, &sv);
	if (ret){ 
		__INFO("Get Key:<%s>--->value is :<%s>", 
				key, 
				sv.data);

		db_free_data(sv.data);
	} else
		__INFO("Get Key:<%s>,but value is NULL", 
				key);

	db_close(db);
}

void _deleteone_test(char *key)
{
	struct slice sk;
	struct nessdb *db;
	char k[KSIZE + 1];
	int len = strlen(key);

	memset(k, 0, KSIZE + 1);
	memcpy(k, key, len);

	db = db_open(DATAS);
	sk.len = (KSIZE + 1);
	sk.data = k;

	db_remove(db, &sk);
	db_close(db);
}

void _scan_test(char *start, char *end, int limit)
{
	int i;
	int ret_c = 0;
	struct slice s1, s2;
	struct nessdb *db;
	struct ness_kv *kv;

	s1.len = strlen(start);
	s1.data = start;
	s2.len = strlen(end);
	s2.data = end;

	db = db_open(DATAS);
	kv = db_scan(db, &s1, &s2, limit, &ret_c);
	for (i = 0; i < ret_c; i++) {
		char *k = kv[i].sk.data;
		char *v = kv[i].sv.data;

		__DEBUG("%s--%s", k, v);
		db_free_data(k);
		db_free_data(v);
	}
	if (kv)
		db_free_data(kv);

	db_close(db);
}

int main(int argc,char** argv)
{
	long int count;

	srand(time(NULL));
	if (argc < 3) {
		fprintf(stderr,"Usage: db-bench <op: write | read> <count>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();
		_write_test(count);
	} else if (strcmp(argv[1], "read") == 0) {
		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();
		
		_read_test(count);
	} else if (strcmp(argv[1], "readone") == 0) {
		_readone_test(argv[2]);
	} else if (strcmp(argv[1], "writeone") == 0) {
		if (argc != 4) {
			fprintf(stderr,"Usage: db-bench writeone <key> <value>\n");
			exit(1);
		}
		_writeone_test(argv[2], argv[3]);
	} else if (strcmp(argv[1], "scan") == 0) {
		if (argc != 5) {
			fprintf(stderr,"Usage: db-bench scan <key-statr> <key-end>\n");
			exit(1);
		}
		_scan_test(argv[2], argv[3], atoi(argv[4]));
	} else if (strcmp(argv[1], "delete") == 0) {
		_deleteone_test(argv[2]);
	} else {
		fprintf(stderr,"Usage: db-bench <op: write | read> <count>\n");
		exit(1);
	}

	return 1;
}
