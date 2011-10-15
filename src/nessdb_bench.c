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
 *   * Neither the name of Redis nor the names of its contributors may be used
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
	 	$cd src
		$make
	 	$./nessdb_bench add
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include "db.h"

#define OP_ADD 1
#define OP_GET 2
#define OP_WALK 3
#define OP_REMOVE 4


#define KEYSIZE 	20
#define VALSIZE 	100
#define NUM 		1000000
#define R_NUM 		10000
#define REMOVE_NUM	10000
#define BUFFERPOOL	(1024*1024*1024)
#define BGSYNC		(1)
#define V		"1.7"
#define LINE 		"+-----------------------+---------------------------+----------------------------------+---------------------+\n"
#define LINE1		"--------------------------------------------------------------------------------------------------------------\n"

static struct timespec start;

static void start_timer(void)
{
        clock_gettime(CLOCK_MONOTONIC, &start);
}

static double get_timer(void)
{
        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);
        long seconds  = end.tv_sec  - start.tv_sec;
        long nseconds = end.tv_nsec - start.tv_nsec;
        return seconds + (double) nseconds / 1.0e9;
}


static char value[VALSIZE+1]={0};
void random_value()
{
	char salt[10]={'1','2','3','4','5','6','7','8','a','b'};
	int i;
	for(i=0;i<VALSIZE;i++)
		value[i]=salt[rand()%10];
}

double _index_size=(double)((double)(KEYSIZE+48)*NUM)/1048576.0;
double _data_size=(double)((double)(VALSIZE+8)*NUM)/1048576.0;
double _query_size=(double)((double)(KEYSIZE+48+VALSIZE+8)*R_NUM)/1048576.0;

void print_header()
{
	printf("Keys:		%d bytes each\n",KEYSIZE);
	printf("Values:		%d bytes each\n",VALSIZE);
	printf("Entries:	%d\n",NUM);
	printf("IndexSize:	%.1f MB (estimated)\n",_index_size);
	printf("DataSize:	%.1f MB (estimated)\n",_data_size);
	printf("BG SYNC:	%s\n",(BGSYNC==1)?"on...":"close...");
	printf(LINE1);
}

void print_environment()
{
	printf("nessDB:		version %s(Multiple && Distributable B+Tree with Level-LRU,Background IO Sync)\n",V);
	time_t now=time(NULL);
	printf("Date:		%s",(char*)ctime(&now));
	
	int num_cpus=0;
	char cpu_type[256]={0};
	char cache_size[256]={0};

	FILE* cpuinfo=fopen("/proc/cpuinfo","r");
	if(cpuinfo){
		char line[1024]={0};
		while(fgets(line,sizeof(line),cpuinfo)!=NULL){
			const char* sep=strchr(line,':');
			if(sep==NULL||strlen(sep)<10)
				continue;
			
			char key[1024]={0};
			char val[1024]={0};
			strncpy(key,line,sep-1-line);
			strncpy(val,sep+1,strlen(sep)-1);
			if(strcmp("model name",key)==0){
				num_cpus++;
				strcpy(cpu_type,val);
			}
			else if(strcmp("cache size",key)==0)
				strncpy(cache_size,val+1,strlen(val)-1);	
		}

		fclose(cpuinfo);
		printf("CPU:		%d * %s",num_cpus,cpu_type);
		printf("CPUCache:	%s\n",cache_size);
	}
}

void db_init_test(int show)
{
	uint64_t sum;
	random_value();

	double cost;
	start_timer();
   	cost=get_timer();
	fprintf(stderr,"loading bloom filter......%30s\r","");

	db_init(BUFFERPOOL,BGSYNC,&sum);

	fflush(stderr);
	
   	cost=get_timer();
	if(show){
		printf(LINE);
		printf("|loadindex	(load:%lld): %.6f sec/op; %.1f reads /sec(estimated) cost:%.2f(sec)\n"
			,sum
			,(double)(cost/R_NUM)
			,(double)(NUM/cost)
			,cost);
	}
}

void db_write_test()
{
	long i,count=0;
	double cost;
	char key[KEYSIZE];
	start_timer();
	for(i=1;i<NUM;i++){
		memset(key,0,sizeof(key));
		sprintf(key,"%dkey",rand()%NUM);
		if(db_add(key,value))
			count++;
		if((i%10000)==0){
			fprintf(stderr,"random write finished %ld ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	
	cost=get_timer();
	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
		,count,(double)(cost/NUM)
		,(double)(NUM/cost)
		,((_index_size+_data_size)/cost)
		,cost);	
}

void db_read_random_test()
{
	long i,count=0;
	long r_start=NUM/2;
	long r_end=r_start+R_NUM;

	double cost;
	char key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++){

		memset(key,0,sizeof(key));
		sprintf(key,"%dkey",rand()%NUM);

		void* data=db_get(key);
		if(data){
			count++;
			free(data);
		}

		if((count%100)==0){
			fprintf(stderr,"random read finished %ld ops%30s\r",count,"");
			fflush(stderr);
		}

	}
   	cost=get_timer();
	printf(LINE);
	printf("|Random-Read	(found:%ld): %.6f sec/op; %.1f reads /sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
		,count
		,(double)(cost/R_NUM)
		,(double)(R_NUM/cost)
		,(_query_size/cost)
		,cost);
}

void db_read_seq_test()
{
	long i, count=0;
	long r_start=NUM/3;
	long r_end=r_start+R_NUM;

	double cost;
	char key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++){
		memset(key,0,sizeof(key));
		sprintf(key,"%ldkey",i);
		void* data=db_get(key);
		if(data){
			count++;
			free(data);
		}

		if((count%1000)==0){
			fprintf(stderr,"sequential read finished %ld ops %30s\r",count,"");
			fflush(stderr);
		}

	}
	cost=get_timer();
	printf(LINE);
	printf("|Seq-Read	(found:%ld): %.6f sec/op; %.1f reads /sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
		,count
		,(double)(cost/R_NUM)
		,(double)(R_NUM/cost)
		,(_query_size/cost)
		,cost);
	
}


void db_remove_test()
{
		
	long i,count=0;
	long r_start=NUM/6;
	long r_end=r_start+REMOVE_NUM;

	double cost;
	char key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++){
		memset(key,0,sizeof(key));
		sprintf(key,"%dkey",rand()%NUM);
		db_remove(key);
		count++;
		if((count%100)==0){
			fprintf(stderr,"random remove  finished %ld ops%30s\r",count,"");
			fflush(stderr);
		}

	}
   	cost=get_timer();
	printf(LINE);
	printf("|Rand-Remove	(done:%ld):	%.6f sec/op;	%.1f reads /sec(estimated);	%.1f MB/sec \n"
		,count
		,(double)(cost/R_NUM)
		,(double)(R_NUM/cost)
		,(_query_size/cost));

}


void db_tests()
{
	db_write_test();
	db_read_seq_test();
	db_read_random_test();
	printf(LINE);
}


int main(int argc,char** argv)
{
	long op;
	int show=1;
	if(argc!=2){
		fprintf(stderr,"Usage: nessdb_benchmark <op>\n");
        	exit(1);
	}

	if(strcmp(argv[1],"add")==0){
		op=OP_ADD;
		show=0;
	}
	else if(strcmp(argv[1],"get")==0)
		op=OP_GET;
	else if(strcmp(argv[1],"walk")==0)
		op=OP_WALK;
	else if(strcmp(argv[1],"remove")==0)
		op=OP_REMOVE;
	else{
		printf("not supported op %s\n", argv[1]);
        	exit(1);
    	}
	
	srand(time(NULL));
	print_header();
	print_environment();
	
	//init
	db_init_test(show);

	if(op==OP_ADD)
		db_tests();
	else if(op==OP_WALK)
		db_read_seq_test();
	else if(op==OP_GET)
		db_read_random_test();
	else if(op==OP_REMOVE)
		db_remove_test();
	//destroy
	db_destroy();
	return 1;
}
