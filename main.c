#include "ivm.h"
#include <stdio.h>
#include <time.h>
#define LOOP 100

int main()
{
	char key[256]={0};
	char value[256]={0};
	int i;

        ivm_init();

	time_t start,end;
	start=clock();
	for(i=0;i<LOOP;i++)
	{
		sprintf(key,"%d",rand());
		sprintf(value,"%d",i);
		ivm_put(key,value);
	}

	end=clock();
	double cost=(double)(end-start);
	printf("%d,cost:%lf\n",LOOP,cost);
	
	return (1);
}
