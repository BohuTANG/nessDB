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
 *   * Neither the name of nessDB nor the names of its contributors may be used
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
#ifndef _GNU_SOURCE
	#define _GNU_SOURCE
#endif
#ifndef _LARGEFILE64_SOURCE
	#define _LARGEFILE64_SOURCE
#endif
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <assert.h>
#ifdef _WIN32
#include <direct.h>
#endif
#include "bitwise.h"
#include "storage.h"
#include "llru.h"
#include "hashes.h"
#include "db.h"
#include "platform.h"

struct info{
	int32_t used;
	int32_t unused;	
};

/*primes are:
 3UL, 5UL, 7UL, 11UL, 13UL, 17UL, 19UL, 23UL, 29UL, 31UL, 37UL,41UL, 43UL, 47UL,
 53UL, 97UL, 193UL, 389UL, 769UL, 1543UL, 3079UL, 6151UL, 12289UL,
 24593UL, 49157UL, 98317UL, 196613UL, 393241UL, 786433UL, 1572869UL,
 3145739UL, 6291469UL, 12582917UL, 25165843UL, 50331653UL,
 100663319UL, 201326611UL, 402653189UL, 805306457UL, 1610612741UL,
 3221225473UL, 4294967291UL
*/
#define DB_SLOT		(13)
#define DB_PREFIX 	"ndbs/ness"
#define DB_DIR    	"ndbs"
#define IDX_PRIME	(16785407)
#define RATIO		(0.618)

static struct btree 	_btrees[DB_SLOT];
static struct info	_infos[DB_SLOT];

void db_init(int bufferpool_size)
{
	int i;	
	int pagepool_size=bufferpool_size*(1-RATIO)/DB_SLOT;
	struct stat st;

	if(stat(DB_DIR, &st) != 0)
		#ifdef _WIN32
		_mkdir(DB_DIR);
		#else
		mkdir(DB_DIR, S_IRWXU | S_IRGRP | S_IROTH);
		#endif
	llru_init(bufferpool_size*RATIO);
	for(i=0;i<DB_SLOT;i++){
		char pre[256]={0};
		snprintf(pre,sizeof pre,"%s%d",DB_PREFIX,i);
		btree_init(&_btrees[i],pre,pagepool_size);
	}
}

int db_add(const char *key,const char *value)
{
	uint32_t off;
	unsigned int slot=djb_hash(key)%DB_SLOT;

	off=btree_insert(&_btrees[slot],key,(const void*)value,strlen(value));
	if(off==0)
		return (0);

	llru_remove(key);
	_infos[slot].used++;
	return (1);
}


void *db_get(const char *key)
{
	void *v;
	int k_l,v_l;
	char *k_tmp,*v_tmp;

	v=llru_get(key);
	if(v==NULL){
		unsigned int slot=djb_hash(key)%DB_SLOT;
		v=btree_get(&_btrees[slot],key);
		if(v==NULL)
			return NULL;
		k_l=strlen(key);
		v_l=strlen((char*)v);

		k_tmp=calloc(k_l,sizeof(char));
		memcpy(k_tmp,key,k_l);

		v_tmp=calloc(v_l,sizeof(char));
		memcpy(v_tmp,(const char*)v,v_l);
		llru_set(k_tmp,v_tmp,k_l,v_l);
		return v;
	}else{
		v_l=strlen((char*)v);
		v_tmp=calloc(v_l,sizeof(char));
		memcpy(v_tmp,(const char*)v,v_l);
		return v_tmp;
	}
}

int db_exists(const char *key)
{
	unsigned int slot=djb_hash(key)%DB_SLOT;
	return btree_get_index(&_btrees[slot],key);
}

void db_remove(const char *key)
{
	int result;
	unsigned int slot=djb_hash(key)%DB_SLOT;
	result=btree_delete(&_btrees[slot],key);
	if(result==0){
		_infos[slot].used--;
		_infos[slot].unused++;
		llru_remove(key);
	}
}


void db_info(char *infos)
{
	uint32_t all_used=0,all_unused=0;
	char str[256];
	struct llru_info linfo;
	for(int i=0;i<DB_SLOT;i++){
		memset(str,0,256);
		snprintf(str,sizeof str,"	db%d:used:<%d>;unused:<%d>;dbsize:<%d>bytes\n",
				i,
				_infos[i].used,
				_infos[i].unused,
				_btrees[i].db_alloc);

		strcat(infos,str);
		all_used+=_infos[i].used;
		all_unused+=_infos[i].unused;
	}
	snprintf(str,sizeof str,"	all-used:<%d>;all-unused:<%d>\n",all_used,all_unused);
	strcat(infos,str);

	llru_info(&linfo);
	memset(str,0,256);
	snprintf(str,sizeof str,
			"	new-level-lru: count:<%d>;"
			"allow-size:<%llu>bytes;"
			"used-size:<%llu>bytes\n"
			"	old-level-lru: count:<%d>;"
			"allow-size:<%llu>bytes;"
			"used-size:<%llu>bytes\n",
			
			linfo.nl_count,
				linfo.nl_allowsize,
				linfo.nl_used,
				linfo.ol_count,
				linfo.ol_allowsize,
				linfo.ol_used);
	strcat(infos,str);
}

void db_destroy()
{
	int i;
	for(i=0;i<DB_SLOT;i++)
		btree_close(&_btrees[i]);
	llru_free();
}
