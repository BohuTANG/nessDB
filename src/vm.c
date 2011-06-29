#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "debug.h"
#include "vm.h"
#include "db.h"
#include "lru.h"
#define DBNAME "ness.db"
#define DBINDEX "ness.idx"

#define MAX_HITS (1024)

typedef struct pointer
{
	char*	k;
	int		index;
	int		offset;
	int		hit_count;
	UT_hash_handle hh;
}pointer_t;

static pointer_t* _pointers;

int _bufsize=0,_db_index=0,_idx_index=0,_lru=0;

void 
vm_init(int lru)
{
	_lru=lru;
	_pointers=NULL;

	db_init(DBNAME,DBINDEX);
}

int
vm_put(char* key,char* value)
{
	char* ktmp;
	pointer_t* vtmp;
	HASH_FIND_STR(_pointers,key,vtmp);
	if(vtmp==NULL)
	{
		 int k_len;
		 int v_len;
		 int db_offset,idx_offset;

		pointer_t* p=(pointer_t*)malloc(sizeof(pointer_t));
		if(p==NULL)
		{
			INFO("mem leak!\n");
			return (-1);
		}

		k_len=strlen(key);
		v_len=strlen(value);

		db_offset=v_len;
		idx_offset=sizeof(int)*2+k_len;
		
		db_write(key,k_len,value,v_len);


		p->index=_db_index;
		p->offset=db_offset;

		//next pointer index
		_db_index+=db_offset;
		_idx_index+=idx_offset;
		//add table
		ktmp=malloc(k_len+1);
		strcpy(ktmp,key);
		p->k=ktmp;
		HASH_ADD_KEYPTR(hh,_pointers,ktmp,k_len,p);
		INFO("vm-put\n");
	}

	return (1);
}

static void
vm_putcache(char* key, int k_len, int v_len)
{
	char* s;
	pointer_t* vtmp;
	HASH_FIND_STR(_pointers,key,vtmp);
	if(vtmp==NULL)
	{
		pointer_t* v=(pointer_t*)malloc(sizeof(pointer_t));
		if(v==NULL)
			INFO("mem leak!\n");
		v->index=_db_index;
		v->offset=v_len;

		s=malloc(k_len+1);
		strcpy(s,key);
		v->k=s;
		HASH_ADD_KEYPTR(hh,_pointers,s,k_len,v);
	}
}

void
vm_load_data()
{
	char k[KLEN__]={0};
	int k_len=0,v_len=0,idx=0,size=0,db_offset=0,idx_offset=0;
	FILE* fin;
	fin=fopen(DBINDEX,"rb");
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

			vm_putcache(k,k_len,v_len);

			idx_offset=(sizeof(int)*2+k_len);
			_idx_index+=idx_offset;

			db_offset=v_len;
			_db_index+=db_offset;


			idx+=(db_offset+idx_offset);
		}
	}
	fclose(fin);
}

int
vm_get(char* key,char* value)
{
	char* lru_v=NULL;
	pointer_t* vtmp;
	HASH_FIND_STR(_pointers,key,vtmp);
	if(vtmp!=NULL)
	{
		if(_lru)
		{
			vtmp->hit_count++;
			lru_v=lru_find(key);
			if(lru_v==NULL)
			{
				db_read(vtmp->index,vtmp->offset,value);
				if(_lru&&(vtmp->hit_count>=MAX_HITS))
				{
					vtmp->hit_count=0;
					lru_add(vtmp->k,value);
					return 2;
				}
			}
		}
		else
				db_read(vtmp->index,vtmp->offset,value);
			
		return 1;
	}
	return 0;
}
