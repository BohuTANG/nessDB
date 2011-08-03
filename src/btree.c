/*
	Dump Compact-Hash-Tree to B-Tree on disk
	Drafted by overred
*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>

#include "btree.h"

#define L0_IDX_NAME "level_0.idx"
#define L1_IDX_NAME "level_1.idx"
#define DB_NAME		"btree_ness.db"


//key:16bytes	+val_offset:4bytes + val_length:4bytes
#define BLOCK_SIZE (16+4+4)

//used:16bytes	+*blocks	+next bucket pointer:4bytes
#define BUCKET_SIZE (4+BLOCK_SIZE*BLOCK_NUM+4)

static btree_t *_btree;


static unsigned int hash(char* str)
{
	unsigned int hash=5381;
	int c;
	while(c=*str++)
		hash=((hash<<5)+hash)+c;//* hash*33+c */

	int l=hash/BUCKET_SIZE;
	hash=(l+1)*BUCKET_SIZE;
	return hash;
}

static int cmp_key(char* k1,char* k2)
{
	int i;
	for(i=0;i<KEY_SIZE;i++)
	{
		if(*k1++!=*k2++)
			return -1;

	}
	return 0;
}

static block_t* blocks_allocate(int capacity)
{
	return (block_t*)calloc(capacity,sizeof(block_t));	
}

void btree_init(int capacity)
{
	int rfd0=open(L0_IDX_NAME, O_RDWR  | O_CREAT, 0644);
	int wfd0=open(L0_IDX_NAME, O_RDWR  | O_CREAT, 0644);

	
	int db_rfd=open(DB_NAME, O_RDWR  | O_CREAT, 0644);
	int db_wfd=open(DB_NAME, O_RDWR  | O_CREAT, 0644);


	if(rfd0<-1 || db_rfd<-1)
	{
		printf("btree_init error\n");
		return;
	}

	pwrite(wfd0,0,BUCKET_SIZE*capacity,0);

	_btree=(btree_t*)malloc(sizeof(btree_t));

	_btree->idx_wfd0=wfd0;
	_btree->idx_rfd0=rfd0;

	_btree->db_wfd=db_wfd;
	_btree->db_rfd=db_rfd;

	_btree->index_offset=0;
	_btree->db_offset=0;
	_btree->cap=capacity;
}

void btree_add(char *key,char *val)
{
	if(_btree->index_offset==0)
	{
		unsigned int h=hash(key);
		int used=0,i;
		bucket_t* bucket=(bucket_t*)malloc(sizeof(bucket_t));
		block_t*  blocks=blocks_allocate(BLOCK_NUM);
		memset(blocks,0,sizeof(block_t)*BLOCK_NUM);

		pread(_btree->idx_rfd0,&used,sizeof(int),h);
		if(used>0)
		{
				if(used>=BLOCK_NUM)
				{
					//printf("err used:%d>%d k:%s hash:%d\n",used,BLOCK_NUM,key,h);	
					return ;
				}

				pread(_btree->idx_rfd0,blocks,sizeof(block_t)*used,h+4);
				for(i=0;i<used;i++)
				{
					int cmp=cmp_key(key,blocks[i].key);
					//printf("k1:%s,k2:%s cmp :%d\n",key,blocks[i].key,cmp);
					if(cmp==0)
					{
						//printf("key has exist!\n");
						//TODO update
						return;
					}
					
				}
		}
	
		
		int v_len=strlen(val);
		block_t * block=&blocks[used];

		memcpy(block->key,key,KEY_SIZE);
		block->val_length=v_len;
		block->val_offset=_btree->db_offset;
		pwrite(_btree->idx_wfd0,block,sizeof(block_t),h+4+BLOCK_SIZE*used);
		
		used+=1;
		pwrite(_btree->idx_wfd0,&used,sizeof(int),h);

		pwrite(_btree->db_wfd,val,v_len,_btree->db_offset);
		_btree->db_offset+=v_len;

	}
}


int main()
{
	int loop=500000;
	char k[16]="1234567890123456";
	char *v="abddddddddddddddddddddddddddddddddddddddabdddddddddddddddddddddddddddddddddddddd";

	btree_init(loop+33);
	int i=0;
	clock_t begin=clock(),end;
	for(i=0;i<loop;i++)
	{
		sprintf(k,"12345679%d",i);
		btree_add(k,v);
	}
	end=clock();
	double cost=end-begin;
	printf("cost:%lf\n",cost);

	return 1;	
}
