/*
	Dump Compact-Hash-Tree to B-Tree on disk
	Drafted by overred
*/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include "btree.h"

#define L0_DB_NAME "level_0"
#define L1_DB_NAME "level_1"

#define BLOCK_NUM 10

// 16 key	+	4 val_offset + 4 val_length
#define BLOCK_SIZE (16+4+4)

//4 used	+	*blocks		+4 next bucket pointer
#define BUCKET_SIZE (4+(16+4+4)*BLOCK_NUM+4)

static btree_t *_btree;


static unsigned int hash(char* str)
{
	unsigned int hash=5381;
	int c;
	while(c=*str++)
		hash=((hash<<5)+hash)+c;//* hash*33+c */
	return hash;
}

static block_t* blocks_allocate(int capacity)
{
	return (block_t*)calloc(capacity,sizeof(block_t));	
}

void btree_init(int capacity)
{
	int fd0=open(L0_DB_NAME, O_RDWR  | O_CREAT, 0644);
	if(fd0<-1)
	{
		printf("btree_init error\n");
		return;
	}

	pwrite(fd0,0,BUCKET_SIZE*capacity,0);

	_btree=(btree_t*)malloc(sizeof(btree_t));

	_btree->fd0=fd0;
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
		pread(_btree->fd0,&used,sizeof(int),h);
		
		block_t* b=blocks_allocate(BLOCK_NUM);
		if(used>0)
		{
				pread(_btree->fd0,b,sizeof(block_t)*used,h+4);
				for(i=0;i<used;i++)
				{
					if(strcmp(key,b[i].key)==0)
					{
						//TODO update
						return;
					}
					
				}
		}

		memcpy(b[used].key,key,KEY_SIZE);
		int v_len=strlen(val);
		b[used].val_offset+=v_len;
		b[used].val_length=v_len;
		pwrite(_btree->fd0,b,sizeof(block_t),h+4+BLOCK_SIZE*used);
		
		used+=1;
		pwrite(_btree->fd0,&used,sizeof(int),h);
			
		if(b)
			free(b);
	}
}


int main()
{
	char *k="1234567890123456";
	char *v="bbc";

	btree_init(3);
	btree_add(k,v);

	return 1;	
}
