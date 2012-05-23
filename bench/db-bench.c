/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
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
#include "../engine/util.h"
#include "../engine/debug.h"
#include "../engine/db.h"

#define TOLOG (0)
#define KSIZE 	16
#define VSIZE 	80
#define BUFFERPOOL	(1024*1024*1024)
#define V			"1.8"
#define LINE 		"+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1		"---------------------------------------------------------------------------------------------------\n"

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

	printf("Keys:		%d bytes each\n", KSIZE);
	printf("Values:		%d bytes each\n", VSIZE);
	printf("Entries:	%d\n", count);
	printf("IndexSize:	%.1f MB (estimated)\n", index_size);
	printf("DataSize:	%.1f MB (estimated)\n", data_size);
	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);
	printf("nessDB:		version %s(LSM-Tree storage engine)\n", V);
	printf("Date:		%s", (char*)ctime(&now));

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
}

void _write_test(long int count)
{
	int i;
	double cost;
	long long start,end;
	struct slice sk, sv;
	struct nessdb *db;
	char *path = getcwd(NULL, 0);

	char key[KSIZE + 1];
	char val[VSIZE + 1];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);

	db = db_open(BUFFERPOOL, path, TOLOG);
	free(path);

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		_random_key(key, KSIZE);
		snprintf(val, VSIZE, "val:%d", i);

		sk.len = KSIZE;
		sk.data = key;
		sv.len = VSIZE;
		sv.data = val;

		db_add(db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", i, "");
			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _read_test(long int count)
{
	int i;
	int ret;
	double cost;
	long long start,end;
	struct slice sk;
	struct slice sv;
	struct nessdb *db;
	char *dir = getcwd(NULL, 0);

	db = db_open(BUFFERPOOL, dir, TOLOG);

	char key[KSIZE + 1];

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);
		_random_key(key, KSIZE);
		sk.len = KSIZE;
		sk.data = key;
		ret = db_get(db, &sk, &sv);
		if (ret) 
			free(sv.data);

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", i, "");
			fflush(stderr);
		}
	}

	db_close(db);
	free(dir);

	end = get_ustime_sec();

	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n"
		,count
		,(double)(cost / count)
		,(double)(count / cost)
		,cost);
}

void _readone_test(char *key)
{
	int ret;
	struct slice sk;
	struct slice sv;
	struct nessdb *db;
	char *dir = getcwd(NULL, 0);
	char k[KSIZE + 1];
	int len = strlen(key);

	memset(k, 0, KSIZE + 1);
	memcpy(k, key, len);

	db = db_open(BUFFERPOOL, dir, TOLOG);
	sk.len = (KSIZE + 1);
	sk.data = k;

	ret = db_get(db, &sk, &sv);
	if (ret){ 
		__INFO("Get Key:<%s>--->value is :<%s>", key, sv.data);
		free(sv.data);
	} else
		__INFO("Get Key:<%s>,but value is NULL", key);

	db_close(db);
	free(dir);
}

int main(int argc,char** argv)
{
	long int count;

	srand(time(NULL));
	if (argc != 3) {
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
	} else {
		fprintf(stderr,"Usage: db-bench <op: write | read> <count>\n");
		exit(1);
	}

	return 1;
}
