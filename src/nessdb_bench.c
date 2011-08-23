/*NOTE:
	How to do
	=========
	 	%cd src
		%make
	 	%./nessdb_bench


	 To have a nocache testing as follows(make sure the ness.ndb files exists):
		%make clean
	 	%make nocache
	 	%echo 3 > /proc/sys/vm/drop_caches
	 	%./nessdb_bench
    
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include "db.h"

#define KEYSIZE 20
#define VALSIZE 100
#define NUM		3000000

//random read num
#define R_NUM		300000
#define V		"1.4"
#define LINE "+-----------------------+---------------------------+----------------------------------------+-----------------------------------+\n"


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


void print_header()
{
	printf("Keys:		%d bytes each\n",KEYSIZE);
	printf("Values:		%d bytes each\n",VALSIZE);
	printf("Entries:	%d\n",NUM);
	printf("RawSize:	%.1f MB (estimated)\n",(double)((double)(KEYSIZE+VALSIZE)*NUM)/1048576.0);
	printf("FileSize:	%.1f MB (estimated)\n",(double)((double)(KEYSIZE+VALSIZE+8*2+4)*NUM)/1048576.0);
	printf("-------------------------------------------------------------------------------------------------------------------------------\n");
}

void print_environment()
{
	printf("nessDB:		version %s(B+Tree)\n",V);
	if(!lru)
		printf("LRU:		closed....\n");
	else
		printf("LRU:		opened....\n");		

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
	db_init(NUM+31,lru);
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
	printf("|write		(succ:%ld):		%lf sec/op;	%lf writes/sec(estimated);	%lf MB/sec \n"
		,count,(double)(cost/NUM)
		,(double)(NUM/cost)
		,(double)((KEYSIZE+VALSIZE+4*2)*NUM/1048576.0/cost));	
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

		if((count%1000)==0)
		{
			fprintf(stderr,"readrandom finished %ld ops%30s\r",count,"");
			fflush(stderr);
		}

	}
   	cost=get_timer();
	printf(LINE);
	printf("|readrandom	(found:%ld):		%lf sec/op;	%lf reads /sec(estimated);	%lf MB/sec \n"
		,count
		,(double)(cost/R_NUM)
		,(double)(R_NUM/cost)
		,(double)((VALSIZE+KEYSIZE+8+8)*R_NUM/1048576.0/cost));
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
	printf("|readseq	(found:%ld):		%lf sec/op;	%lf reads /sec(estimated);	%lf MB/sec\n"
		,count
		,(double)(cost/R_NUM)
		,(double)(R_NUM/cost)
		,(double)((VALSIZE+KEYSIZE+8+8)*R_NUM/1048576.0/cost));
	
}



void db_tests()
{
	db_init_test();
	db_write_test();
	db_read_seq_test();
	db_read_random_test();
	printf(LINE);
}

void nocache_read_random_test()
{
	db_init_test();
	db_read_random_test();
	printf(LINE);
}


int main()
{
	srand(time(NULL));
	print_header();
	print_environment();
#ifdef NOCACHE
	nocache_read_random_test();
#else
	db_tests();
#endif
	return 1;
}
