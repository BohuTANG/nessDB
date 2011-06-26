#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "vm.h"
#include "lru.h"
#include "debug.h"

#define LOOP 5

void vm_init_test(int lru)
{
	vm_init(lru);
	INFO("---init---");
}

void vm_load_data_test()
{
	vm_load_data();
}

void vm_put_test()
{
	nessobj_t* obj=(nessobj_t*)malloc(sizeof(nessobj_t));
	obj->k=("key1");
	obj->v=("val1");
	vm_put(obj);

	nessobj_t* obj1=(nessobj_t*)malloc(sizeof(nessobj_t));
	obj1->k=("key2");
	obj1->v=("val2");
	vm_put(obj1);
	INFO("---vm put ---");
	
}

void vm_get_test()
{
	nessobj_t* obj=(nessobj_t*)malloc(sizeof(nessobj_t));
	obj->k=("key2");
	obj->v=malloc(sizeof(char)*1024);

	int ret=vm_get(obj);
	if(ret)
	{
		LOG("v is %s",obj->v);
	}
}

void vm_bulk_put_test()
{
	double cost;
	int i;
	//nessobj_t* obj=(nessobj_t*)malloc(sizeof(nessobj_t));
	//obj->k=malloc(sizeof(char)*256);
	//obj->v=malloc(sizeof(char)*256);
	clock_t begin,end;
	begin=clock();
	for(i=0;i<LOOP;i++)
	{
		nessobj_t* obj=(nessobj_t*)malloc(sizeof(nessobj_t));
		obj->k=malloc(sizeof(char)*256);
		obj->v=malloc(sizeof(char)*256);
		sprintf(obj->k,"xxxxxxxxxxxxxxxxx%d",i);
		sprintf(obj->v,"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx%d",i);
		vm_bulk_put(obj);
	}
	vm_bulk_flush();
	end=clock();
	cost=(double)(end-begin);
	printf("put cost:%lf\n",cost);
	INFO("---vm bulk put ---");
}

void vm_bulk_get_test()
{
	int i;
	double cost;
	//nessobj_t* obj=(nessobj_t*)malloc(sizeof(nessobj_t));
	//obj->k=malloc(sizeof(char)*256);
	//obj->v=malloc(sizeof(char)*256);
	
	clock_t begin,end;
	begin=clock();
	for(i=1;i<LOOP;i++)
	{
		nessobj_t* obj=(nessobj_t*)malloc(sizeof(nessobj_t));
		obj->k=malloc(sizeof(char)*256);
		obj->v=malloc(sizeof(char)*256);
		sprintf(obj->k,"xxxxxxxxxxxxxxxxx%d",i);
		int  ret=vm_get(obj);
		if(ret)
			printf("v is:%s",obj->v);
		else
			printf("nofound!");
	}
	end=clock();
    	cost=(double)(end-begin);
	printf("get cost:%lf\n",cost);
}

void lru_test()
{
	double cost;
	int i;
	char key[256]={0};
	clock_t begin,end;
	begin=clock();
	for(i=0;i<LOOP;i++)
	{
		sprintf(key,"xxxxx%d",i);

		char* v=lru_find(key);
		if(!v)
			lru_add(key,key);
	}
	end=clock();
	cost=(double)(end-begin);
	printf("lru cost:%lf\n",cost);
}

int main()
{
	int lru=0;
	//lru_test();
	vm_init_test(lru);
	//vm_put_test();
	//vm_get_test();
	vm_bulk_put_test();
	vm_bulk_get_test();
	//vm_load_data_test();
	//vm_get_test();
	return (1);
}
