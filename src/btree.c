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

#define PAGE_SIZE 	(sizeof(page_t))
#define X 		(29989)
#define ROOT_GAP 	(((X/PAGE_SIZE)+1)*PAGE_SIZE)

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

static unsigned int slot_hash(char* str)
{
	unsigned int hash=5381;
	int c;
	while(c=*str++)
		hash=((hash<<5)+hash)+c;//* hash*33+c */
		
   	unsigned int mod=hash%X;	
	unsigned int l=mod/PAGE_SIZE;
	mod=(l+1)*PAGE_SIZE;
	return mod;
}

static void flush_data(page_t *page,size_t page_offset,blob_t *blob)
{
	//page
	fseek(_btree->idx_w,page_offset,SEEK_SET);
	fwrite(page,sizeof(page_t),1,_btree->idx_w);
	_btree->idx_offset+=sizeof(page_t);

	//value	
	fseek(_btree->db_w,_btree->db_offset,SEEK_SET);
	fwrite(blob->val,blob->len,1,_btree->db_w);
	_btree->db_offset+=blob->len;
}

static void  split_page(page_t *page,size_t page_offset)
{
	int new_used=page->used/2;
	int child=_btree->idx_offset;


	//write new child  page
	page_t *new_page=(page_t*)malloc(sizeof(page_t));
	new_page->used=(BLOCK_NUM-new_used);
	printf("used:%d,new_used:%d",page->used,new_used);

	memcpy(new_page->blocks,&page->blocks[new_used],(BLOCK_NUM-new_used)*BLOCK_SIZE);
	fseek(_btree->idx_w,0,SEEK_END);
	fwrite(new_page,sizeof(page_t),1,_btree->idx_w);
	if(new_page)
		free(new_page);

	//update
	_btree->idx_offset+=sizeof(page_t);

	//write old page
	page->used=new_used;
	page->blocks[new_used-1].child=child;

	memset(&page->blocks[new_used],0,(BLOCK_NUM-new_used+1)*BLOCK_SIZE);

	fseek(_btree->idx_w,page_offset,SEEK_SET);
	fwrite(page,sizeof(page_t),1,_btree->idx_w);

//	printf("new child page offset:%d,new-used:%d\n",child,new_used);
}


static size_t insert_page(size_t page_offset,char *key,blob_t *blob)
{
	page_t* page=(page_t*)malloc(sizeof(page_t));

	fseek(_btree->idx_r,page_offset,SEEK_SET);
	fread(page,sizeof(page_t),1,_btree->idx_r);
	block_t* blocks=page->blocks;

	int left=0,right=page->used-1;
	while(left<=right)
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
		child=insert_page(child,key,blob);			
	}	

	memmove(&blocks[i],&blocks[i],sizeof(block_t)*(page->used-i-1));

	page->used+=1;

	block_t *block=&blocks[i];
	memcpy(block->key,key,KEY_SIZE);
	block->val_offset=_btree->db_offset;
	block->val_length=blob->len;
	block->child=0;

	//flush index and data to file
	flush_data(page,page_offset,blob);
	printf("add-->key:%s,slot:%d,used:%d,val_off:%d\n",key,page_offset,page->used,block->val_offset);
	if(page->used>(BLOCK_NUM-1))
	{	
		printf("BLOCK_NUM:%d\n",BLOCK_NUM);
		split_page(page,page_offset);
	}
	
	//free
	if(page)
	{
		free(page);
		page=NULL;
	}

	return child;
}


static size_t lookup(char *key,size_t page_offset)
{
	while(page_offset)
	{
		page_t* page=(page_t*)malloc(sizeof(page_t));
		fseek(_btree->idx_r,page_offset,SEEK_SET);
		fread(page,sizeof(page_t),1,_btree->idx_r);
		int used=0;
		block_t* blocks=page->blocks;

		printf("get-->key:%s,slot:%d,used:%d\n",key,page_offset,page->used);
		int left=0,right=page->used-1;
		while(left<=right)
		{
			size_t i=(left+right)/2;
			int cmp=strcmp(key,blocks[i].key);
			if(cmp==0)
			{
				int off=blocks[i].val_offset;
				if(page)
					free(page);
		//		printf("get-->key:%s,slot:%d,used:%d,off:%d\n",key,page_offset,page->used,off);
				return off;
			}
			if(cmp<0)
				right=(i-1);
			else
				left=(i+1);
		}
		size_t i=left;
		page_offset=blocks[i].child;
		if(page)
			free(page);
	}
	return 0;
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


void btree_add(char *key,char *val)
{
	unsigned int h=slot_hash(key);
	blob_t *blob=(blob_t*)malloc(sizeof(blob_t));
	blob->len=strlen(val);
	blob->val=val;

	insert_page(h,key,blob);

	if(blob)
		free(blob);
}


int btree_get(char *key,char *val)
{
	unsigned int h=slot_hash(key);
	size_t offset=lookup(key,h);
	printf("get offset:%d\n",offset);
}

void btree_destroy()
{
	fclose(_btree->idx_r);
	fclose(_btree->idx_w);
	fclose(_btree->db_r);
	fclose(_btree->db_w);
	if(_btree)
		free(_btree);
}


int main(int argc,char** argv)
{
	if(argc!=3)
	{
		fprintf(stderr,"Usage: btree <op> <count> \n");
		exit(1);
	}

	int loop=atoi(argv[2]);
	char k[16]="88abcdefghigklmn";
	char *v="abddddddddddddddddddddddddddddddddddddddabdddddddddddddddddddddddddddddddddddddd";

	btree_init(loop+33);
	int i=0;
	if(strcmp(argv[1],"add")==0)
	{
		clock_t begin=clock(),end;
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
		end=clock();
		double cost=end-begin;
		printf("add cost:%lf\n",cost);
	}

	if(strcmp(argv[1],"get")==0)
	{
		clock_t begin=clock(),end;
		for(i=0;i<loop;i++)
		{
			char val[1024]={0};
			sprintf(k,"%dabcdefg",i);
			btree_get(k,val);
		}
		end=clock();
		double cost=end-begin;
		printf("get cost:%lf\n",cost);
	}

	btree_destroy();
	return 1;	
}
