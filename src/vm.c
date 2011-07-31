#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "hashtable.h"
#include "vm.h"
#define DBNAME "ness.db"
#define DBINDEX "ness.idx"

#define MAX_HITS (1024)

typedef struct db
{
	FILE*	db_write_ptr;
	FILE*	db_read_ptr;
	FILE*	db_write_idx_ptr;
	FILE*	db_read_idx_ptr;
}db_t;

typedef struct pointer
{
	int		index;
	int		offset;
	int     idx_offset;
}pointer_t;

static db_t* _db;
static pointer_t* _pointers;
static hashtable*	_ht;

int _bufsize=0,_db_index=0,_idx_index=0,_lru=0;

//get H bit
static int get_H(int x)
{
	if((x&0x80000000)!=0x80000000)
		return 0;
	else
		return 1;
}

//set H bit to 0
static int set_H_0(int x)
{
	return	x&=0x3FFFFFFF;
}

//set H bit to 1
static int set_H_1(int x)
{
	return x|=(0x01<<31);	
}

void 
vm_init(int capacity)
{
	_pointers=NULL;
	_ht=hashtable_create(capacity);

	FILE* fread=NULL,*fwrite=NULL,*fread_idx=NULL,*fwrite_idx=NULL;
	fread=fopen(DBNAME,"rb");
	fread_idx=fopen(DBINDEX,"rb");
	if(fread==NULL)
	{
		fwrite=fopen(DBNAME,"wb");
		fread=fopen(DBNAME,"rb");

		fwrite_idx=fopen(DBINDEX,"wb");
		fread_idx=fopen(DBINDEX,"wb");

	}
	else
	{
		fwrite=fopen(DBNAME,"ab");
		fwrite_idx=fopen(DBINDEX,"ab");
	}

	_db=(db_t*)malloc(sizeof(db_t));
	if(fread!=NULL
		&&fwrite!=NULL
		&&fwrite_idx!=NULL
		&&_db!=NULL)
	{
		_db->db_read_ptr=fread;
		_db->db_write_ptr=fwrite;

		_db->db_write_idx_ptr=fwrite_idx;
		_db->db_read_idx_ptr=fread_idx;
	}
	else
		printf("db_init error!");
}

int
vm_put(char* key,char* value)
{
	pointer_t* vtmp=NULL;
	if(vtmp==NULL)
	{
		 int k_len;
		 int v_len;
		 int db_offset,idx_offset;

		pointer_t* p=(pointer_t*)malloc(sizeof(pointer_t));
		if(p==NULL)
		{
			printf("mem leak!\n");
			return (-1);
		}

		k_len=strlen(key);
		v_len=strlen(value);

		db_offset=v_len;
		idx_offset=sizeof(int)*2+k_len;
		
		fwrite(&k_len,1,sizeof(int),_db->db_write_idx_ptr);
		fwrite(&v_len,1,sizeof(int),_db->db_write_idx_ptr);
		fwrite(key,k_len,1,_db->db_write_idx_ptr);
		fwrite(value,v_len,1,_db->db_write_ptr);


		p->index=_db_index;
		p->offset=db_offset;
		p->idx_offset=_idx_index;

		//next pointer index
		_db_index+=db_offset;
		_idx_index+=idx_offset;
		//add table
		hashtable_set(_ht,key,p);
	}

	return (1);
}

static void
vm_putcache(char* key, int k_len, int v_len)
{
	pointer_t* vtmp=NULL;
	if(vtmp==NULL)
	{
		pointer_t* v=(pointer_t*)malloc(sizeof(pointer_t));
		if(v==NULL)
			printf("mem leak!\n");
		v->index=_db_index;
		v->offset=v_len;
		v->idx_offset=_idx_index;

		hashtable_set(_ht,key,v);
	}
}

void
vm_load_index()
{
	char k[1024]={0};
	int k_len=0,v_len=0,size=0,db_offset=0,idx_offset=0;
	FILE* fin=_db->db_read_idx_ptr;
	fseek (fin , 0 , SEEK_END);
	size = ftell (fin);
	rewind (fin);
	while(_idx_index<size)
	{
		int r1=fread(&k_len,sizeof(int),1,fin);
		int r2=fread(&v_len,sizeof(int),1,fin);
		int r3=fread(k,k_len,1,fin);

		if(get_H(k_len)==0)
			vm_putcache(k,k_len,v_len);
		else
			k_len=set_H_0(k_len);

		idx_offset=(sizeof(int)*2+k_len);
		_idx_index+=idx_offset;

		db_offset=v_len;
		_db_index+=db_offset;
	}
}

int
vm_get(char* key,char* value)
{
	char* lru_v=NULL;
	pointer_t* vtmp;
	vtmp=hashtable_get(_ht,key);
	if(vtmp!=NULL)
	{
		fseek(_db->db_read_ptr,vtmp->index,SEEK_SET);
		int i=fread(value,vtmp->offset,1,_db->db_read_ptr);
		return 1;
	}
	return 0;
}

void
vm_remove(char* key)
{
	pointer_t* vtmp;
	vtmp=hashtable_get(_ht,key);
	if(vtmp!=NULL)
	{
		int k_len=0;
		fseek(_db->db_read_idx_ptr,vtmp->idx_offset,SEEK_SET);
		int r1=fread(&k_len,sizeof(int),1,_db->db_read_idx_ptr);

		k_len=set_H_1(k_len);
		fwrite(&k_len,1,sizeof(int),_db->db_write_idx_ptr);

		hashtable_remove(_ht,key);
	}
}

void vm_destroy()
{
	fclose(_db->db_read_ptr);	
	fclose(_db->db_write_ptr);	
	fclose(_db->db_read_idx_ptr);	
	fclose(_db->db_write_idx_ptr);	
}
