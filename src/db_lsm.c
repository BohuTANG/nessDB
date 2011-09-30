 /* Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <assert.h>
#include <pthread.h>

#include "bitwise.h"
#include "storage.h"
#include "db_lsm.h"
#include "llru.h"

#define LOG_0	"ness.log0"
#define LOG_1	"ness.log1"

#define LOG_MAXSIZE	(1024*1024*2)

#define IDX_PRIME	(16785407)

#define LOG(format,...) fprintf(stderr," -> " format "\n",__VA_ARGS__)

static struct btree 	_btree;
static struct bloom	_bloom;

volatile short		_status;
static int		_cur_used;
static int		_flag;

static struct log	**_logs;
static struct log	*_cur_log;
static pthread_t	_bgmerge;

/*Sequential read,and mark keys in BloomFilter
* This is very important to preheat datas.
*/
static void db_loadbloom()
{
	int i,l,super_size=sizeof(struct btree_super);
	uint64_t alloc=_btree.alloc-super_size,offset;
	int newsize=(sizeof(struct btree_table));
	lseek64(_btree.fd,super_size, SEEK_SET);
	while(alloc>0){
		struct btree_table *table=malloc(newsize);
		int r=read(_btree.fd,table, newsize) ;
		if(table->size>0){
			for(l=0;l<table->size;l++){
				if(get_H(offset)==0)
					bloom_add(&_bloom,table->items[l].sha1);
			}
		}
		free(table);
		alloc-=newsize;
	}
}

static uint32_t getsize(int fd) {
    struct stat sb;

    if (fstat(fd,&sb) == -1) return 0;
    return sb.st_size;
}

void *bgmerge_func();
void *bgmerge_func()
{
	while(1)
	{
		struct log *log;
		if((_status&0x01)==0x01&&_logs[0]->used>0)
			log=_logs[0];
		else if(((_status>>1)&0x01)==0x01&&_logs[1]->used>0)
			log=_logs[1];
		else
			continue;

		int klen=0,vlen=0;
		uint32_t fsize=getsize(log->fd);
		uint32_t tmp_size=fsize;
		uint32_t nsize=sizeof(struct log_node);

		lseek(log->fd,0,SEEK_SET);
		int i=0;
		while(tmp_size>0){
			struct log_node *node=malloc(nsize);
			if(read(log->fd,node,nsize)!= nsize){
				fprintf(stderr, "read log: I/O error\n");
				abort();
			}

			//LOG("klen:[%d] vlen:[%d] voff:[%d] key:%s ",node->k_len,node->v_len,node->v_offset,node->key);
			char *value=malloc(node->v_len+1);
			if(read(log->fd,value,node->v_len)!=node->v_len){
				fprintf(stderr, "read log: I/O error\n");
				abort();
			}

			if((i%10000)==0){
				fprintf(stderr,"btree insert %ld ops%30s\r",i,"");
				fflush(stderr);
			}
			btree_insert(&_btree,(const uint8_t*)(node->key),value,node->v_len);
			i++;
			tmp_size-=(nsize+node->v_len);
			free(node);
			free(value);
		}
		log->fd= open(LOG_0,O_RDWR | O_TRUNC | O_CREAT | O_BINARY, 0644);
		log->used=0;
		LOG("...fd%d truncate....",log->flag);
		_status&=log->magic;
	}
}


static void log_init()
{
	_logs=calloc(2,sizeof(struct log*));

	struct log *log1=calloc(1,sizeof(struct log));
	_logs[0]=log1;
	_logs[0]->fd= open(LOG_0,O_RDWR | O_TRUNC | O_CREAT | O_BINARY, 0644);
	_logs[0]->used=0;
	_logs[0]->flag=0;
	_logs[0]->magic=0x01;

	struct log *log2=calloc(1,sizeof(struct log));
	_logs[1]=log2;
	_logs[1]->fd = open(LOG_1,O_RDWR | O_TRUNC | O_CREAT | O_BINARY, 0644);
	_logs[1]->used=0;
	_logs[1]->flag=1;
	_logs[1]->magic=0x02;

	_cur_log=_logs[0];
	_cur_used=0;
	_status=0;
	_flag=0;

	int t1=pthread_create(&_bgmerge,NULL,bgmerge_func,NULL);
	pthread_join(&t1,NULL);
}

static void log_add(char *key,char *value)
{
	int k_len=strlen(key);
	int v_len=strlen(value);
//	uint64_t v_offset=btree_insert_data(&_btree,value,v_len);

	uint32_t nsize=sizeof(struct log_node);
	struct log_node *node=calloc(1,nsize);
	node->k_len=k_len;
	node->v_len=v_len;
//	node->v_offset=v_offset;
	memcpy(node->key,key,20);
	if (write(_cur_log->fd,node,nsize) != nsize) {
		fprintf(stderr, "log: I/O error\n");
		abort();
	}
	
	if (write(_cur_log->fd,value,v_len) != v_len) {
		fprintf(stderr, "log: I/O error\n");
		abort();
	}

	free(node);

	_cur_log->used+=(nsize+v_len);
	if(_cur_log->used>LOG_MAXSIZE){
		
		fsync(_cur_log->fd);
		_status|=_cur_log->magic;	
		do{
			_cur_log=_cur_log->flag==0?_logs[1]:_logs[0];
		}while((_status&_cur_log->magic)!=0);
	}

}

void db_init(int bufferpool_size)
{
	btree_init(&_btree);
	llru_init(bufferpool_size);
	bloom_init(&_bloom,IDX_PRIME);
	db_loadbloom();	

	log_init();
}


int db_add(char* key,char* value)
{

	log_add(key,value);	
	bloom_add(&_bloom,key);
	return (1);
}


void *db_get(char* key)
{
	int b=bloom_get(&_bloom,(const char*)key);
	if(b!=0)
		return NULL;

	void *v=llru_get((const char*)key);
	if(v==NULL){
		v=btree_get(&_btree,key);
		char *k_tmp=strdup(key);
		char *v_tmp=strdup((char*)v);
		llru_set(k_tmp,v_tmp,strlen(k_tmp),strlen(v_tmp));
		return v;
	}else{
		return strdup((char*)v);
	}
}

void db_remove(char* key)
{
	btree_delete(&_btree,key);
	llru_remove(key);
}

void db_destroy()
{
	llru_free();
	btree_close(&_btree);
}
