#include "ivm.h"
#include <stdio.h>
#include <time.h>


int main()
{
	char key[256]={0};
	char value[256]={0};
	int i;

        ivm_init();

	time_t start,end;
	start=clock();
	for(i=0;i<100000;i++)
	{
		sprintf(key,"%d",rand());
		sprintf(value,"%d",i);
		ivm_put(key,value);
	}

	end=clock();
	double cost=(double)(end-start);
	printf("cost:%lf\n",cost);
	
	return (1);
}
