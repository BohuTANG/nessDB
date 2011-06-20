#include "ivm.h"
#include "db.h"
#include <stdlib.h>
#include <string.h>


static pointer_t* _pointers;

void ivm_init()
{
	_pointers=NULL;

	const char* dbname="ness.db";
	db_init(dbname);
	
}


int ivm_put(char* key,char* value)
{
	pointer_t* v;
	HASH_FIND_STR(_pointers,key,v);
	if(v==NULL)
		{
			pointer_t* p=(pointer_t*)malloc(sizeof(pointer_t));
			if(p==NULL)
			{
				printf("mem leak!\n");
				return (-1);
			}

			HASH_ADD_KEYPTR(hh,_pointers,strdup(key),strlen(key),p);
			db_write(key,value);
		}

	return (1);
}

int ivm_bulk_put(char* key,char* value)
{
	return (1);
}
