/* Dump Compact-Hash-Tree to B-Tree on disk
   Drafted by overred
*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>

#include "btree.h"

#define L0_IDX_NAME "dump.idx"
#define L1_IDX_NAME "level_1.idx"
#define DB_NAME		"dump.db"

//key:16bytes	+val_offset:4bytes + val_length:4bytes
//#define BLOCK_SIZE (16+4+4)

//used:16bytes	+*blocks	+next bucket pointer:4bytes
#define BUCKET_SIZE (sizeof(bucket_t))

typedef struct btree
{
	FILE *idx_r;
	FILE *idx_w;
	FILE *db_r;
	FILE *db_w;

	int db_offset;
	int capacity;
}btree_t;

static btree_t *_btree;

static unsigned int hash(char* str)
{
	unsigned int hash=5381;
	int c;
	while(c=*str++)
		hash=((hash<<5)+hash)+c;//* hash*33+c */
		
    unsigned int mod=hash%29989;	
	unsigned int l=mod/BUCKET_SIZE;
	mod=(l+1)*BUCKET_SIZE;
	return mod;
}


static block_t* blocks_allocate(int capacity)
{
	return (block_t*)calloc(capacity,sizeof(block_t));	
}

void btree_init(int capacity)
{
	FILE *idx_r=fopen(L0_IDX_NAME,"a+");
	FILE *idx_w=fopen(L0_IDX_NAME,"wb");

	FILE *db_r=fopen(DB_NAME,"a+");
	FILE *db_w=fopen(DB_NAME,"wb");

	_btree=(btree_t*)malloc(sizeof(btree_t));
	_btree->idx_r=idx_r;
	_btree->idx_w=idx_w;

	_btree->db_r=db_r;
	_btree->db_w=db_w;

	_btree->capacity=capacity;
	_btree->db_offset=0;
}


static void flush_bucket(size_t bucket_offset,size_t i,block_t *block,size_t used,char *val,size_t v_len)
{
	used+=1;
	fseek(_btree->idx_w,bucket_offset,SEEK_SET);
	fwrite(&used,sizeof(int),1,_btree->idx_w);
	
	fseek(_btree->idx_w,BLOCK_SIZE*i,SEEK_CUR);
	fwrite(block,sizeof(block_t),1,_btree->idx_w);

	
	fseek(_btree->db_w,_btree->db_offset,SEEK_CUR);
	fwrite(val,v_len,1,_btree->db_w);
	_btree->db_offset+=v_len;

}

static size_t insert_bucket(size_t bucket_offset,char *key,char *val,size_t val_len)
{
	bucket_t* bucket=(bucket_t*)malloc(sizeof(bucket_t));
	block_t*  blocks=blocks_allocate(BLOCK_NUM);

	int used=0;
	fseek(_btree->idx_r,bucket_offset,SEEK_SET);
	fread(blocks,sizeof(block_t),used,_btree->idx_r);
	
	int left=0,right=used;
	while(left<right)
	{
		size_t i=(left+right)/2;
		int cmp=strcmp(key,blocks[i].key);
		if(cmp==0)
		{
			//TODO update	
		}
		if(cmp<0)
			right=(i-1);
		else
			left=(i+1);
	}

	size_t i=left;
	int child=blocks[i].child;
	if(child)
	{
		child=insert_bucket(child,key,val,val_len);			
	}
	
	if(used>=BLOCK_NUM)
	{
		//TODO split bucket	
	}

	memmove(&blocks[i+1],&blocks[i],sizeof(block_t)*(used-i));

	block_t *block=&blocks[i];
	memcpy(block->key,key,KEY_SIZE);
	block->val_length=val_len;
	block->child=0;

	//flush
	flush_bucket(bucket_offset,i,block,used,val,val_len);

	//free
	if(bucket)
		free(bucket);
	if(blocks)
		free(blocks);

	return child;
}

void btree_add(char *key,char *val)
{
	unsigned int h=hash(key);
		
	int v_len=strlen(val);
	insert_bucket(h,key,val,v_len);
}


int main()
{
	int loop=1000;
	char k[16]="88abcdefghigklmn";
	char *v="abddddddddddddddddddddddddddddddddddddddabdddddddddddddddddddddddddddddddddddddd";

	btree_init(loop+33);
	int i=0;
	clock_t begin=clock(),end;
	for(i=1;i<loop;i++)
	{
		sprintf(k,"abcdefg%zd",i);
		btree_add(k,v);
		if((i%10000)==0)
		{
			fprintf(stderr,"finished %d ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	end=clock();
	double cost=end-begin;
	printf("cost:%lf\n",cost);
	return 1;	
}
