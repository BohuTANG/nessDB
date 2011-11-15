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
	 	$cd src
		$make
	 	$./nessdb-bench add
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include "../nessDB/db.h"
#include "../nessDB/platform.h"

#define OP_ADD 1
#define OP_GET 2
#define OP_WALK 3
#define OP_REMOVE 4


#define KEYSIZE 	16
#define VALSIZE 	80
#define NUM 		2000000
#define R_NUM 		20000
#define REMOVE_NUM	20000
#define BUFFERPOOL	(1024*1024*1024)
#define V			"1.8"
#define LINE 		"+-----------------------+---------------------------+----------------------------------+---------------------+\n"
#define LINE1		"--------------------------------------------------------------------------------------------------------------\n"


#include <sys/time.h>
static struct timeval  start;

static void start_timer(void)
{
        gettimeofday(&start, NULL);
}

static double get_timer(void)
{
        struct timeval end;
        gettimeofday(&end, NULL);
        long seconds = end.tv_sec - start.tv_sec;
        long nseconds = (end.tv_usec - start.tv_usec) * 1000;
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

void random_key(char *key,int length) {
    	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";
	memset(key,0,length);
	for (int i = 0; i < length; i++)
		key[i] = salt[rand() % length];
}

double _index_size=(double)((double)(KEYSIZE+8)*NUM)/1048576.0;
double _data_size=(double)((double)(VALSIZE+4)*NUM)/1048576.0;
double _query_size=(double)((double)(KEYSIZE+8+VALSIZE+4)*R_NUM)/1048576.0;

void print_header()
{
	printf("Keys:		%d bytes each\n",KEYSIZE);
	printf("Values:		%d bytes each\n",VALSIZE);
	printf("Entries:	%d\n",NUM);
	printf("IndexSize:	%.1f MB (estimated)\n",_index_size);
	printf("DataSize:	%.1f MB (estimated)\n",_data_size);
	printf(LINE1);
}

void print_environment()
{
	printf("nessDB:		version %s(Multiple && Distributable B+Tree with Level-LRU,Page-Cache)\n",V);
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

nessDB *db_init_test(int show)
{
	random_value();
	return db_init(BUFFERPOOL);
}

void db_write_test(nessDB *db)
{
	long i,count=0;
	double cost;
	char key[KEYSIZE];
	start_timer();
	for(i=1;i<NUM;i++){
		random_key(key,KEYSIZE);
		if(db_add(db, key, value)==1)
			count++;
		if((i%5000)==0){
			fprintf(stderr,"random write finished %ld ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	
	cost=get_timer();
	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
		,count,(double)(cost/NUM)
		,(double)(count/cost)
		,((_index_size+_data_size)/cost)
		,cost);	
}

void db_read_random_test(nessDB *db)
{
	long i,count=0;
	long r_start=NUM/2;
	long r_end=r_start+R_NUM;

	double cost;
	char key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++){
		random_key(key,KEYSIZE);
		void* data=db_get(db, key);
		if(data){
			count++;
			free(data);
		}

		if((count%1000)==0){
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

void db_read_seq_test(nessDB *db)
{
	long i, count=0;
	long r_start=NUM/3;
	long r_end=r_start+R_NUM;

	double cost;
	char key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++){
		random_key(key,KEYSIZE);
		void* data=db_get(db, key);
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


void db_remove_test(nessDB *db)
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
		db_remove(db, key);
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


void db_tests(nessDB *db)
{
	db_write_test(db);
	db_read_random_test(db);
	db_read_seq_test(db);
	printf(LINE);
}


int main(int argc,char** argv)
{
	long op;
	int show=1;
	nessDB *db;
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
	
	db = db_init_test(show);

	if(op==OP_ADD)
		db_tests(db);
	else if(op==OP_WALK)
		db_read_seq_test(db);
	else if(op==OP_GET)
		db_read_random_test(db);
	else if(op==OP_REMOVE)
		db_remove_test(db);
	db_destroy(db);
	return 1;
}
