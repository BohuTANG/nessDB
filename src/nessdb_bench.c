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
#define NUM 		10000000
#define R_NUM 		10000
#define REMOVE_NUM	10000
#define V		"1.6"
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

static int lru=0;
static char value[VALSIZE+1]={0};
void random_value()
{
	char salt[10]={'1','2','3','4','5','6','7','8','a','b'};
	int i;
	for(i=0;i<VALSIZE;i++)
	{
		value[i]=salt[rand()%10];
	}
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
	printf(LINE1);
}

void print_environment()
{
	printf("nessDB:		version %s(B+ Tree)\n",V);
	time_t now=time(NULL);
	printf("Date:		%s",(char*)ctime(&now));
	
	int num_cpus=0;
	char cpu_type[256]={0};
	char cache_size[256]={0};

	FILE* cpuinfo=fopen("/proc/cpuinfo","r");
	if(cpuinfo)
	{
		char line[1024]={0};
		while(fgets(line,sizeof(line),cpuinfo)!=NULL)
		{
			const char* sep=strchr(line,':');
			if(sep==NULL||strlen(sep)<10)
				continue;
			
			char key[1024]={0};
			char val[1024]={0};
			strncpy(key,line,sep-1-line);
			strncpy(val,sep+1,strlen(sep)-1);
			if(strcmp("model name",key)==0)
			{
				num_cpus++;
				strcpy(cpu_type,val);
			}
			else if(strcmp("cache size",key)==0)
			{
				strncpy(cache_size,val+1,strlen(val)-1);	
			}
		}

		fclose(cpuinfo);
		printf("CPU:		%d * %s",num_cpus,cpu_type);
		printf("CPUCache:	%s\n",cache_size);
	}
}

void db_init_test()
{
	random_value();

	double cost;
	start_timer();
   	cost=get_timer();
	fprintf(stderr,"loading index......%30s\r","");
	db_init();
	fflush(stderr);

   	cost=get_timer();
	printf(LINE);
	printf("|loadindex	(load:%d): %.6f sec/op; %.1f reads /sec(estimated) cost:%.2f(sec)\n"
		,NUM
		,(double)(cost/R_NUM)
		,(double)(NUM/cost)
		,cost);
}

void db_write_test()
{
	long i,count=0;
	double cost;
	uint8_t key[KEYSIZE];
	start_timer();
	for(i=0;i<NUM;i++)
	{
		memset(key,0,sizeof(key));
		sprintf(key,"%ldkey",i);
		if(db_add(key,value))
			count++;
		if((i%10000)==0)
		{
			fprintf(stderr,"write finished %ld ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	
	cost=get_timer();
	printf(LINE);
	printf("|write		(succ:%ld): %.6f sec/op; %.1f writes/sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
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
	uint8_t key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++)
	{

		memset(key,0,sizeof(key));
		sprintf(key,"%ldkey",rand()%i);
		void* data=db_get(key);
		if(data)
			count++;
		else
			printf("nofound!%s\n",key);
		free(data);

		if((count%100)==0)
		{
			fprintf(stderr,"readrandom finished %ld ops%30s\r",count,"");
			fflush(stderr);
		}

	}
   	cost=get_timer();
	printf(LINE);
	printf("|readrandom	(found:%ld): %.6f sec/op; %.1f reads /sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
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
	uint8_t key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++)
	{
		memset(key,0,sizeof(key));
		sprintf(key,"%ldkey",i);
		void* data=db_get(key);
		if(data)
			count++;
		else
			printf("nofound!%s\n",key);
		free(data);

		if((count%1000)==0)
		{
			fprintf(stderr,"readseq finished %ld ops %30s\r",count,"");
			fflush(stderr);
		}

	}
	cost=get_timer();
	printf(LINE);
	printf("|readseq	(found:%ld): %.6f sec/op; %.1f reads /sec(estimated); %.1f MB/sec; cost:%.3f(sec)\n"
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
	uint8_t key[KEYSIZE]={0};
	start_timer();
	for(i=r_start;i<r_end;i++)
	{

		memset(key,0,sizeof(key));
		sprintf(key,"%ldkey",rand()%i);
		db_remove(key);
		count++;
		if((count%100)==0)
		{
			fprintf(stderr,"remove random finished %ld ops%30s\r",count,"");
			fflush(stderr);
		}

	}
   	cost=get_timer();
	printf(LINE);
	printf("|removerandom	(found:%ld):	%.6f sec/op;	%.1f reads /sec(estimated);	%.1f MB/sec \n"
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
	long i,count,op;
	if(argc!=2)
	{
		fprintf(stderr,"Usage: nessdb_benchmark <op>\n");
        	exit(1);
	}

	if(strcmp(argv[1],"add")==0)
		op=OP_ADD;
	else if(strcmp(argv[1],"get")==0)
		op=OP_GET;
	else if(strcmp(argv[1],"walk")==0)
		op=OP_WALK;
	else if(strcmp(argv[1],"remove")==0)
		op=OP_REMOVE;
	else
	{
		printf("not supported op %s\n", argv[1]);
        	exit(1);
    	}
	
	srand(time(NULL));
	print_header();
	print_environment();
	
	//init
	db_init_test();

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
