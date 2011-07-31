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
#include <time.h>
#include <string.h>
#include "vm.h"

#define KEYSIZE 16
#define VALSIZE 84
#define NUM		1000000
#define V		"1.3"
#define LINE "+-----------------------+-------------------+----------------------------------------+-------------------+\n"

static char value[VALSIZE+1]={0};
void random_value()
{
	char salt[10]={'1','2','3','4','5','6','7','8','a','b'};
	int i;
	for(i=0;i<VALSIZE;i++)
	{
		value[i]=salt[random()%10];
	}
}


void print_header()
{
	printf("Keys:		%d bytes each\n",KEYSIZE);
	printf("Values:		%d bytes each\n",VALSIZE);
	printf("Entries:	%d\n",NUM);
	printf("RawSize:	%.1f MB (estimated)\n",(double)((KEYSIZE+VALSIZE)*NUM)/1048576.0);
	printf("FileSize:	%.1f MB (estimated)\n",(double)((KEYSIZE+VALSIZE+4*2)*NUM)/1048576.0);
	printf("----------------------------------------------------------------------------------------------------------\n");
}

void print_environment()
{
	printf("nessDB:		version %s\n",V);
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
			if(sep==NULL||strlen(sep)<=2)
				continue;
			
			char key[512]={0};
			char val[512]={0};
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

void vm_init_test()
{
	random_value();
	vm_init(NUM+31);
}

void vm_write_test()
{
	int i;
	double cost;
	clock_t begin,end;
	begin=clock();
	char key[256]="key1";

	for(i=0;i<NUM;i++)
	{
		sprintf(key,"%dxxxxxxxxxx",i);
		vm_put(key,value);
		if((i%10000)==0)
		{
			fprintf(stderr,"write finished %d ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	
	end=clock();
	cost=(double)(end-begin);
	printf(LINE);
	printf("|write:			%lf micros/op; |	%lf writes/sec(estimated); |	%lf MB/sec |\n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(KEYSIZE+VALSIZE+4*2)*NUM/1048576.0/cost));	
}

void vm_read_random_test()
{
	int count=0;
	double cost;
	char key[256]="xxxxxxxxxxxxxxxxx2";
	int i;
	clock_t begin,end;
	begin=clock();
	for(i=0;i<NUM;i++)
	{
	  	char val[1024]={0};
	
		if(i==0)
			sprintf(key,"0xxxxxxxxxx");
		else
			sprintf(key,"%dxxxxxxxxxx",rand()%i);
		int ret=vm_get(key,val);
		if(ret)
			count++;
		else
			printf("nofound!%s\n",key);

		if((i%10000)==0)
		{
			fprintf(stderr,"readrandom finished %d ops%30s\r",i,"");
			fflush(stderr);
		}

	}
	end=clock();
    cost=(double)(end-begin);
	printf(LINE);
	printf("|readrandom:		%lf micros/op; |	%lf reads /sec(estimated); |	%lf MB/sec |\n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(VALSIZE+4)*NUM/1048576.0/cost));
}

void vm_read_seq_test()
{
	int count=0;
	double cost;
	char key[256]="xxxxxxxxxxxxxxxxx2";
	int i;
	clock_t begin,end;
	begin=clock();
	for(i=0;i<NUM;i++)
	{
	  	char val[1024]={0};
		sprintf(key,"%dxxxxxxxxxx",i);
		int ret=vm_get(key,val);
		if(ret)
			count++;
		else
			printf("nofound!%s\n",key);

		if((i%10000)==0)
		{
			fprintf(stderr,"readseq finished %d ops %30s\r",i,"");
			fflush(stderr);
		}

	}
	end=clock();
	cost=(double)(end-begin);
	printf(LINE);
	printf("|readseq:		%lf micros/op; |	%lf reads /sec(estimated); |	%lf MB/sec |\n",(double)(cost/NUM),(double)(NUM/cost)*1000000.0,(double)(1000000.0*(VALSIZE+4)*NUM/1048576.0/cost));
	
}


void vm_tests()
{
	vm_init_test();
	vm_write_test();
	vm_read_random_test();
	vm_read_seq_test();
	printf(LINE);
}

void nocache_read_random_test()
{
	vm_init_test();
	vm_load_index();
	vm_read_random_test();
	printf(LINE);
}


int main()
{
	print_header();
	print_environment();
#ifdef NOCACHE
	nocache_read_random_test();
#else
	vm_tests();
#endif
	return 1;
}
