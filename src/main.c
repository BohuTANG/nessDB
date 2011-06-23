#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "vm.h"
#include "debug.h"

#define LOOP 10

void vm_init_test()
{
	vm_init();
	INFO("---init---");
}

void vm_put_test()
{
	char* key="key1";
	char* value="val1";
	vm_put(key,value);
	INFO("---vm put ---");
	
}

void vm_bulk_put_test()
{
	int i;
	char key[256]={0};
	char value[256]={0};
	for(i=0;i<LOOP;i++)
	{
		sprintf(key,"%d",i);
		sprintf(value,"%d",i);
		vm_bulk_put(key,value);
	}
	vm_bulk_flush();
	INFO("---vm bulk put ---");
}

void vm_get_test()
{
	char key[256]="0";
	char value[256]={0};
	int  ret=vm_get(key,value);
	if(ret)
		LOG("v is:%s",value);
	else
		INFO("nofound!");
}


int main()
{
    vm_init_test();
	//vm_put_test();
	vm_bulk_put_test();
	vm_get_test();
	return (1);
}
