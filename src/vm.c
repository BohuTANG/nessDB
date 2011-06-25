#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "debug.h"
#include "vm.h"
#include "db.h"
#include "io.h"
#include "lru.h"
#define DBNAME "ness.db"
#define BULK_SIZE (4096*64)

typedef struct pointer
{
	unsigned int index;
	unsigned int offset;
	UT_hash_handle hh;
}pointer_t;

static pointer_t* _pointers;
static io_t* _io;

int _bufsize=0,_dbidx=0,_lru=0;

void 
vm_init(int lru)
{
	_lru=lru;
	_pointers=NULL;
	_io=io_new(0);

	db_init(DBNAME);
}

int
vm_put(char* key,char* value)
{
	char* ktmp;
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
			ktmp=malloc(k_len+1);
			strcpy(ktmp,key);
			HASH_ADD_KEYPTR(hh,_pointers,ktmp,k_len,p);
			INFO("vm-put\n");
		}

	return (1);
}

static void
vm_putcache(char* key, int k_len, int v_len,int offset)
{
	char* s;
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
		s=malloc(k_len+1);
		strcpy(s,key);
		HASH_ADD_KEYPTR(hh,_pointers,s,k_len,p);
	}
}

int
vm_bulk_put(char* key,char* value)
{
	int k_len=strlen(key);
	int v_len=strlen(value);
	int offset=sizeof(int)*2+k_len+v_len;
	_bufsize+=offset;

	vm_putcache(key,k_len,v_len,offset);

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
vm_load_data()
{
	char k[KLEN__]={0};
	int k_len=0,v_len=0,idx=0,size=0,offset=0;
	FILE* fin;
	fin=fopen(DBNAME,"rb");
	if(fin)
	{
		fseek (fin , 0 , SEEK_END);
		size = ftell (fin);
		rewind (fin);
		while(idx<size)
		{
			fread(&k_len,sizeof(int),1,fin);
			fread(&v_len,sizeof(int),1,fin);
			fread(k,k_len,1,fin);
			fseek(fin,v_len,SEEK_CUR);

			offset=(sizeof(int)*2+k_len+v_len);
			vm_putcache(k,k_len,v_len,offset);
			idx+=offset;
		}
		_dbidx+=size;
	}
	fclose(fin);
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
	char* lru_v=NULL;
	pointer_t* v;
	HASH_FIND_STR(_pointers,key,v);
	if(v!=NULL)
	{
		if(_lru)
			lru_v=lru_find(key);
		if(lru_v==NULL)
		{
			db_read(v->index,v->offset,value);
			if(_lru)
				lru_add(key,value);
		}
		return 1;
	}
	return 0;
}
