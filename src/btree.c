/* HASHB-Trees:replace the root-level with hash
   Drafted by overred
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "btree.h"

#define L0_IDX_NAME 	"dump_btree.idx"
#define DB_NAME		"dump_btree.db"

#define PAGE_SIZE 	(sizeof(page_t))
#define X 		(29989)
#define ROOT_GAP 	(((X/PAGE_SIZE)+1)*PAGE_SIZE)


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
	_btree->idx_offset=ROOT_GAP;
}


static void split_page(page_t *page)
{
	size_t new_used=page->used/2;
	//old-page not write then,(+PAGE_SIZE) for gap
	int idx_offset=_btree->idx_offset+PAGE_SIZE;
	page_t new_page;
	memset(&new_page,0,PAGE_SIZE);

	new_page.used=(BLOCK_NUM-new_used);
	memcpy(new_page.blocks,&page->blocks[new_used],new_page.used*BLOCK_SIZE);
	fseek(_btree->idx_w,idx_offset,SEEK_SET);
	fwrite(&new_page,PAGE_SIZE,1,_btree->idx_w);
//	fflush(_btree->idx_w);
	_btree->idx_offset+=PAGE_SIZE*2;

	//old page
	page->used=new_used;
	page->blocks[new_used-1].child=idx_offset;
	memset(&page->blocks[new_used],0,new_page.used*BLOCK_SIZE);
}

static void insert_page(size_t page_offset,char *key,blob_t *blob)
{
	size_t i=0;
	page_t page;
	block_t *blocks=NULL;
	while(page_offset)
	{
		memset(&page,0,PAGE_SIZE);
		fseek(_btree->idx_r,page_offset,SEEK_SET);
		fread(&page,PAGE_SIZE,1,_btree->idx_r);
		blocks=page.blocks;
		int left=0,right=page.used;
		while(left<right)
		{
			i=(left+right)/2;
			int cmp=strcmp(key,page.blocks[i].key);
			if(cmp<0)
				right=i;
			else if(cmp>0)
				left=i+1;
			else
				return;
		}

		i=left;
		//ugly:boundary opt,if last block has child ,the i =i+1
		if(page.used!=0
			&&i==page.used
			&&page.blocks[i-1].child!=0)
			i-=1;

		int child=page.blocks[i].child;
		if(child>0)
			page_offset=child;
		else
			break;
	}

	if((page.used)!=i)
		memmove(&blocks[i+1],&blocks[i],(page.used-i)*BLOCK_SIZE);
	page.used+=1;

	memcpy(blocks[i].key,key,KEY_SIZE);
	blocks[i].val_offset=_btree->db_offset;
	blocks[i].child=0;

	if(page.used>BLOCK_NUM-1)
		split_page(&page);

	//index
	fseek(_btree->idx_w,page_offset,SEEK_SET);
	fwrite(&page,PAGE_SIZE,1,_btree->idx_w);
	//fflush(_btree->idx_w);

	//data
	fwrite(&blob->len,sizeof(size_t),1,_btree->db_w);
	_btree->db_offset+=4;
	fwrite(blob->val,blob->len,1,_btree->db_w);
	_btree->db_offset+=blob->len;
	//fflush(_btree->db_w);
}

void btree_add(char *key,char *val)
{
	unsigned int h=slot_hash(key);
	blob_t *blob=(blob_t*)malloc(sizeof(blob_t));
	memset(blob,0,sizeof(blob_t));

	blob->len=strlen(val);
	blob->val=val;

	insert_page(h,key,blob);

	if(blob)
		free(blob);
}


static size_t lookup(size_t page_offset,char *key)
{
	while(page_offset)
	{
		size_t i=0;
		page_t page;
		memset(&page,0,PAGE_SIZE);
		fseek(_btree->idx_r,page_offset,SEEK_SET);
		fread(&page,PAGE_SIZE,1,_btree->idx_r);
		if(page.used==0)
			return -1;

		int left=0,right=page.used;
		while(left<right)
		{
			int i=(left+right)/2;
			int cmp=strcmp(key,page.blocks[i].key);
			if(cmp<0)
				right=i;
			else if(cmp>0)
				left=i+1;
			else
				return page.blocks[i].val_offset;
		}

		i=left;

		//ugly
		if(page.used!=0&&
			i==page.used&&
			page.blocks[i-1].child!=0)
			i-=1;

		page_offset=page.blocks[i].child;
		if(page_offset==0)
				break;
	}

	return -1;
}

int btree_get(char *key,char *val)
{
	unsigned int h=slot_hash(key);
	int offset=lookup(h,key);
	if(offset>=0)
	{
		size_t len=0;
		fseek(_btree->db_r,offset,SEEK_SET);
		fread(&len,sizeof(size_t),1,_btree->db_r);

		fread(val,len,1,_btree->db_r);
		return 1;
	}

	return 0;
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
	char k[16]={0};
	char v[1024]={0};

	btree_init(loop+33);
	int i=0;
	if(strcmp(argv[1],"add")==0)
	{
		clock_t begin=clock(),end;
		for(i=0;i<loop;i++)
		{
			sprintf(k,"%dabcdefg",i);
			sprintf(v,"abdcdaddasfasffffffffffffffffffffasdfasddddddddddddddddddddddddddddddddddddddd%d",i);
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
