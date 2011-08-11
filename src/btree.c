/* HASHB-Trees:replace the root-level with hash
   		Drafted by overred
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "btree.h"

#define L0_IDX_NAME 	"dump.idx"
#define L1_IDX_NAME 	"level_1.idx"
#define DB_NAME		"dump.db"

#define BUCKET_SIZE 	(sizeof(bucket_t))
#define X 		(29989)
#define ROOT_GAP 	(((X/BUCKET_SIZE)+1)*BUCKET_SIZE)

typedef struct blob
{
	size_t len;
	char* val;
}blob_t;

typedef struct btree
{
	FILE *idx_r;
	FILE *idx_w;
	FILE *db_r;
	FILE *db_w;

	int db_offset;
	int idx_offset;
	int capacity;
}btree_t;

static btree_t *_btree;

static unsigned int bucket_hash(char* str)
{
	unsigned int hash=5381;
	int c;
	while(c=*str++)
		hash=((hash<<5)+hash)+c;//* hash*33+c */
		
   	unsigned int mod=hash%X;	
	unsigned int l=mod/BUCKET_SIZE;
	mod=(l+1)*BUCKET_SIZE;
	return mod;
}

static void flush_bucket(bucket_t *bucket,size_t bucket_offset,blob_t *blob)
{
	//bucket
	fseek(_btree->idx_w,bucket_offset,SEEK_SET);
	fwrite(bucket,sizeof(bucket_t),1,_btree->idx_w);
	_btree->idx_offset+=sizeof(bucket_t);

	//value	
	fseek(_btree->db_w,_btree->db_offset,SEEK_SET);
	fwrite(blob->val,blob->len,1,_btree->db_w);
	_btree->db_offset+=blob->len;
}

static void  split_bucket(bucket_t *bucket,size_t bucket_offset)
{
	int new_used=bucket->used/2;
	int child=_btree->idx_offset;


	//write new child  bucket
	bucket_t *new_bucket=(bucket_t*)malloc(sizeof(bucket_t));
	new_bucket->used=(BLOCK_NUM-new_used);
	memcpy(new_bucket->blocks,&bucket->blocks[new_used],(BLOCK_NUM-new_used)*BLOCK_SIZE);
	fseek(_btree->idx_w,0,SEEK_END);
	fwrite(new_bucket,sizeof(bucket_t),1,_btree->idx_w);
	if(new_bucket)
		free(new_bucket);

	//update
	_btree->idx_offset+=sizeof(bucket_t);

	//write old bucket
	bucket->used=new_used;
	bucket->blocks[new_used-1].child=child;
	memset(&bucket->blocks[new_used],0,(BLOCK_NUM-new_used)*BLOCK_SIZE);

	fseek(_btree->idx_w,bucket_offset,SEEK_SET);
	fwrite(bucket,sizeof(bucket_t),1,_btree->idx_w);

//	printf("new child bucket offset:%d,new-used:%d\n",child,new_used);
}

void btree_init(int capacity)
{
	FILE *idx_r=fopen(L0_IDX_NAME,"rb"),*idx_w;
	if(idx_r==NULL)
	{
		idx_w=fopen(L0_IDX_NAME,"wb");
		idx_r=fopen(L0_IDX_NAME,"rb");
	}
	else
		idx_w=fopen(L0_IDX_NAME,"ab");


	FILE *db_r=fopen(DB_NAME,"r+"),*db_w;
	if(db_r==NULL)
	{
		db_w=fopen(DB_NAME,"wb");
		db_r=fopen(DB_NAME,"rb");
	}
	else
		db_w=fopen(DB_NAME,"ab");

	
	_btree=(btree_t*)malloc(sizeof(btree_t));
	_btree->idx_r=idx_r;
	_btree->idx_w=idx_w;

	_btree->db_r=db_r;
	_btree->db_w=db_w;

	_btree->capacity=capacity;
	_btree->db_offset=0;
	_btree->idx_offset=0;
}


static size_t insert_bucket(size_t bucket_offset,char *key,blob_t *blob)
{
	bucket_t* bucket=(bucket_t*)malloc(sizeof(bucket_t));

	fseek(_btree->idx_r,bucket_offset,SEEK_SET);
	fread(bucket,sizeof(bucket_t),1,_btree->idx_r);
	block_t* blocks=bucket->blocks;

	int left=0,right=bucket->used-1;
	while(left<=right)
	{
		size_t i=(left+right)/2;
		int cmp=strcmp(key,blocks[i].key);
		if(cmp==0)
		{
			//TODO update
			if(bucket)
				free(bucket);
			return bucket_offset;
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
		child=insert_bucket(child,key,blob);			
	}	

	memmove(&blocks[i+1],&blocks[i],sizeof(block_t)*(bucket->used-i));

	bucket->used+=1;

	block_t *block=&blocks[i];
	memcpy(block->key,key,KEY_SIZE);
	block->val_offset=_btree->db_offset;
	block->val_length=blob->len;
	block->child=0;

	//flush index and data to file
	flush_bucket(bucket,bucket_offset,blob);
	printf("add-->key:%s,slot:%d,used:%d,val_off:%d\n",key,bucket_offset,bucket->used,block->val_offset);
	if(bucket->used>(BLOCK_NUM-1))
		split_bucket(bucket,bucket_offset);
	
	//free
	if(bucket)
		free(bucket);

	return child;
}

void btree_add(char *key,char *val)
{
	unsigned int h=bucket_hash(key);
	blob_t *blob=(blob_t*)malloc(sizeof(blob_t));
	blob->len=strlen(val);
	blob->val=val;

	insert_bucket(h,key,blob);

	if(blob)
		free(blob);
}


static size_t lookup(char *key,size_t bucket_offset)
{
	while(bucket_offset)
	{
		bucket_t* bucket=(bucket_t*)malloc(sizeof(bucket_t));
		fseek(_btree->idx_r,bucket_offset,SEEK_SET);
		fread(bucket,sizeof(bucket_t),1,_btree->idx_r);
		int used=0;
		block_t* blocks=bucket->blocks;

		printf("get-->key:%s,slot:%d,used:%d\n",key,bucket_offset,bucket->used);
		int left=0,right=bucket->used-1;
		while(left<=right)
		{
			size_t i=(left+right)/2;
			int cmp=strcmp(key,blocks[i].key);
			if(cmp==0)
			{
				int off=blocks[i].val_offset;
				if(bucket)
					free(bucket);
		//		printf("get-->key:%s,slot:%d,used:%d,off:%d\n",key,bucket_offset,bucket->used,off);
				return off;
			}
			if(cmp<0)
				right=(i-1);
			else
				left=(i+1);
		}
		size_t i=left;
		bucket_offset=blocks[i].child;
		if(bucket)
			free(bucket);
	}
	return 0;
}

int btree_get(char *key,char *val)
{
	unsigned int h=bucket_hash(key);
	size_t offset=lookup(key,h);
	printf("get offset:%d\n",offset);
}


int main()
{
	int loop=10;
	char k[16]="88abcdefghigklmn";
	char *v="abddddddddddddddddddddddddddddddddddddddabdddddddddddddddddddddddddddddddddddddd";

	btree_init(loop+33);
	int i=0;
	clock_t begin=clock(),end;
#ifdef ADD
	for(i=0;i<loop;i++)
	{
		sprintf(k,"%dabcdefg",i);
		btree_add(k,v);
		if((i%10000)==0)
		{
			fprintf(stderr,"finished %d ops%30s\r",i,"");
			fflush(stderr);
		}
	}
#else
	for(i=0;i<loop;i++)
	{
		char val[1024]={0};
		sprintf(k,"%dabcdefg",i);
		btree_get(k,val);
	}
#endif

	end=clock();
	double cost=end-begin;
	printf("cost:%lf\n",cost);
	return 1;	
}
