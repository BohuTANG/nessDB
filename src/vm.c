#include <stdlib.h>
#include <string.h>
#include "debug.h"
#include "vm.h"
#include "db.h"
#include "io.h"

#define BULK_SIZE (4096*10)

typedef struct pointer
{
	unsigned int index;
	unsigned int offset;
	UT_hash_handle hh;
}pointer_t;

static pointer_t* _pointers;
static io_t* _io;

unsigned int _bufsize=0,_dbidx=0;

void 
vm_init()
{
	const char* dbname="ness.db";
	_pointers=NULL;
	_io=io_new(0);

	db_init(dbname);
	
}


int
vm_put(char* key,char* value)
{
	pointer_t* v;
	HASH_FIND_STR(_pointers,key,v);
	if(v==NULL)
		{
			 int k_len;
			 int v_len;
			 int offset;

			pointer_t* p=(pointer_t*)malloc(sizeof(pointer_t));
			if(p==NULL)
			{
				INFO("mem leak!\n");
				return (-1);
			}

			k_len=strlen(key);
			v_len=strlen(value);
			offset=sizeof(int)*2+k_len+v_len;
			db_write(key,k_len,value,v_len);
			p->index=_dbidx;
			p->offset=offset;

			//next pointer index
			_dbidx+=offset;
			//add table
			HASH_ADD_KEYPTR(hh,_pointers,key,k_len,p);
			INFO("vm-put\n");
		}

	return (1);
}

static void
vm_putcache(char* key, int k_len,char* value, int v_len,int offset)
{
	pointer_t* v;
	HASH_FIND_STR(_pointers,key,v);
	if(v==NULL)
	{
		pointer_t* p=(pointer_t*)malloc(sizeof(pointer_t));
		if(p==NULL)
			INFO("mem leak!\n");
		p->index=_dbidx;
		p->offset=offset;
		_dbidx+=offset;
		//add table
		HASH_ADD_KEYPTR(hh,_pointers,strdup(key),k_len,p);
	}
}

int
vm_bulk_put(char* key,char* value)
{
	int k_len=strlen(key);
	int v_len=strlen(value);
	int offset=sizeof(int)*2+k_len+v_len;
    _bufsize+=offset;

	//put cache
	vm_putcache(key,k_len,value,v_len,offset);

	io_puti(_io,k_len);
	io_puti(_io,v_len);
	io_put(_io,key,k_len);
	io_put(_io,value,v_len);
	if(_bufsize>BULK_SIZE)
	{
		int b_len=io_len(_io);
		if(b_len>0)
		{
			char* block=io_detach(_io);
			db_bulk_write(block,b_len);
			_bufsize=0;
			if(block)
				free(block);
			INFO("bulk write...");

		}
	}

	return (1);
}

void
vm_bulk_flush()
{
	int b_len=io_len(_io);
	if(b_len>0)
	{
		char* block=io_detach(_io);
		db_bulk_write(block,b_len);
		_bufsize=0;
		if(block)
			free(block);
		INFO("bulk flush...");
	}

}

int
vm_get(char* key,char* value)
{
	pointer_t* v;
	HASH_FIND_STR(_pointers,key,v);
	if(v!=NULL)
	{
		db_read(v->index,v->offset,value);
		return 1;
	}
	return 0;
}
