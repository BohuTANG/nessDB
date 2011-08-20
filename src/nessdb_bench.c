/*NOTE:
	How to do
	=========
	 	%cd src
		%make
	 	%./nessdb_bench


	 To have a nocache testing as follows(make sure the ness.idx and ness.db files exists):
		%make clean
	 	%make nocache
	 	%echo 3 > /proc/sys/vm/drop_caches
	 	%./nessdb_bench
    
	The benchmark results without caches almost didn't change. 
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include "db.h"

#define KEYSIZE 32
#define VALSIZE 100
#define NUM		5000000
#define V		"1.4"
#define LINE "+-----------------------+-------------------+----------------------------------------+-------------------+\n"

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
	printf("FileSize:	%.1f MB (estimated)\n",(double)((double)(KEYSIZE+VALSIZE+4*2)*NUM)/1048576.0);
	printf("----------------------------------------------------------------------------------------------------------\n");
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
	int i;
	double cost;
	clock_t begin,end;
	begin=clock();
	char key[KEYSIZE]={0};

	for(i=0;i<NUM;i++)
	{
		sprintf(key,"%dxxxxxxxxx",i);
		db_add(key,value);
		if((i%10000)==0)
		{
			fprintf(stderr,"write finished %d ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	
	end=clock();
	cost=(double)(end-begin);
	printf(LINE);
	printf("|write:		%lf micros/op;	%lf writes/sec(estimated);	%lf MB/sec \n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(KEYSIZE+VALSIZE+4*2)*NUM/1048576.0/cost));	
}

void db_read_random_test()
{
	int count=0;
	double cost;
	char key[KEYSIZE]={0};
	int i;
	clock_t begin,end;
	begin=clock();
	for(i=0;i<NUM;i++)
	{
	
		if(i==0)
			sprintf(key,"0xxxxxxxxx");
		else
			sprintf(key,"%dxxxxxxxxx",rand()%i);
		void* data=db_get(key);
		if(data)
			count++;
		else
			printf("nofound!%s\n",key);
		free(data);

		if((i%10000)==0)
		{
			fprintf(stderr,"readrandom finished %d ops%30s\r",i,"");
			fflush(stderr);
		}

	}
	end=clock();
    cost=(double)(end-begin);
	printf(LINE);
	printf("readrandom:	%lf micros/op;	%lf reads /sec(estimated);	%lf MB/sec \n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(VALSIZE+4)*NUM/1048576.0/cost));
}

void db_read_seq_test()
{
	int count=0;
	double cost;
	char key[KEYSIZE]={0};
	int i;
	clock_t begin,end;
	begin=clock();
	for(i=0;i<NUM;i++)
	{
		sprintf(key,"%dxxxxxxxxx",i);
		void* data=db_get(key);
		if(data)
			count++;
		else
			printf("nofound!%s\n",key);
		free(data);

		if((i%10000)==0)
		{
			fprintf(stderr,"readseq finished %d ops %30s\r",i,"");
			fflush(stderr);
		}

	}
	end=clock();
	cost=(double)(end-begin);
	printf(LINE);
	printf("|readseq:	%lf micros/op;	%lf reads /sec(estimated);	%lf MB/sec\n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(VALSIZE+4)*NUM/1048576.0/cost));
	
}


void db_remove_random_test()
{
	int count=0;
	double cost;
	char key[KEYSIZE]={0};
	int i;
	clock_t begin,end;
	begin=clock();
	for(i=1;i<NUM;i++)
	{
		sprintf(key,"%dxxxxxxxxx",rand()%i);
		db_remove(key);

		if((i%10000)==0)
		{
			fprintf(stderr,"remove finished %d ops %30s\r",i,"");
			fflush(stderr);
		}

	}
	end=clock();
	cost=(double)(end-begin);
	printf(LINE);
	printf("|removerandom:	%lf micros/op;	%lf reads /sec(estimated);	%lf MB/sec \n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(VALSIZE+4)*NUM/1048576.0/cost));
	
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
	db_load_index();
	db_read_random_test();
	printf(LINE);
}


int main()
{
	print_header();
	print_environment();
#ifdef NOCACHE
	nocache_read_random_test();
#else
	db_tests();
#endif
	return 1;
}
