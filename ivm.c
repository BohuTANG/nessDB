#include "ivm.h"
#include "db.h"
#include <stdlib.h>
#include <string.h>
#define BULK_SIZE 4096

size_t _index=0,_offset=0;
char _buffer[BULK_SIZE+1]={0};//bulk bufer

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

			size_t k_len=strlen(key);
			size_t v_len=strlen(value);
			size_t offset=sizeof(int)+k_len+v_len;
			size_t idx= db_write(key,k_len,value,v_len,offset);
			p->index=idx;
			p->offset=offset;
			//add table
			HASH_ADD_KEYPTR(hh,_pointers,key,k_len,p);
			printf("ivm-put\n");
		}

	return (1);
}

int ivm_bulk_put(char* key,char* value)
{
	
	return (1);
}
