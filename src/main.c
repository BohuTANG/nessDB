#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "vm.h"
#include "debug.h"

#define LOOP 5

void vm_init_test()
{
	vm_init();
	INFO("---init---");
}

void vm_load_data_test()
{
	vm_load_data();
}

void vm_put_test()
{
	char* key="key1";
	char* value="val1";
	vm_put(key,value);

	key="key2";
	value="val2";
	vm_put(key,value);
	INFO("---vm put ---");
	
}

void vm_bulk_put_test()
{
	double cost;
	int i;
	char key[256]={0};
	char value[256]={0};
	clock_t begin,end;
	begin=clock();
	for(i=0;i<LOOP;i++)
	{
		sprintf(key,"xxxxxxxxxxxxxxxxx%d",i);
		sprintf(value,"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx%d",i);
		vm_bulk_put(key,value);
	}
	vm_bulk_flush();
	end=clock();
    cost=(double)(end-begin);
	printf("put cost:%lf\n",cost);
	INFO("---vm bulk put ---");
}

void vm_get_test()
{
	double cost;
	char key[256]="key2";
	char value[256]={0};
	int i;
		clock_t begin,end;
	begin=clock();
	for(i=0;i<LOOP;i++)
	{
		int  ret=vm_get(key,value);
		if(ret)
			LOG("v is:%s",value);
		else
			INFO("nofound!");
	}
	end=clock();
    cost=(double)(end-begin);
	printf("get cost:%lf\n",cost);
}


int main()
{
    vm_init_test();
	//vm_put_test();
	vm_load_data_test();
	vm_bulk_put_test();
	vm_get_test();
	return (1);
}
