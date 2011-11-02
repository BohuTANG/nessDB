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

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <time.h>
#include <string.h>
#include "bloom.h"
#include "ht.h"
#include "db.h"

#define LOG(format,...) printf("\033[1;34m --->" format "\033[0m\n", __VA_ARGS__);

void test_bloom_init()
{
	struct bloom _b;
	bloom_init(&_b,1000);
	assert(0 == 0);
	LOG(" %s PASSED....","bloom init");
}

void test_bloom_add()
{
	const char *k="yes";
	struct bloom _b;
	bloom_init(&_b,1000);
	bloom_add(&_b,k);
	int ret=bloom_get(&_b,k);
	assert(ret == 0);
	LOG("%s PASSED....","bloom add");

}

void test_bloom_get()
{
	const char *k="yes";
	struct bloom _b;
	bloom_init(&_b,1000);
	bloom_add(&_b,k);
	int ret=bloom_get(&_b,"no");
	assert(ret ==-1);
	LOG("%s PASSED....","bloom get");
}


void test_ht_int()
{
	int k=1,k1=2;
	struct ht _ht;
	ht_init(&_ht,13,INT);

	ht_set(&_ht,&k,&k);
	ht_set(&_ht,&k1,&k1);
	void* ret=ht_get(&_ht,&k);
	int i=*((int*)ret);

	ret=ht_get(&_ht,&k1);
	i=*((int*)ret);
	assert(i==k1);
	LOG("%s PASSED....","hashtable<int> get");


	ht_remove(&_ht,&k);
	ret=ht_get(&_ht,&k);
	assert(ret == NULL );
	ht_free(&_ht);	

	LOG("%s PASSED....","hashtable<int> remove");
}

void test_ht_string()
{
	char *k="k",*k1="k1";
	struct ht _ht;
	ht_init(&_ht,13,STRING);

	ht_set(&_ht,k,k);
	ht_set(&_ht,k1,k1);

	void* ret=ht_get(&_ht,k);
	assert(strcmp(ret,k) == 0);
	LOG("%s PASSED....","hashtable<string> get");

	ret=ht_get(&_ht,k1);
	assert(strcmp(ret,k1)== 0);
	
	ht_remove(&_ht,k);
	ret=ht_get(&_ht,k);
	assert(ret==NULL);

	ht_free(&_ht);	
	LOG("%s PASSED....","hashtable<string> remove");
}

void test_db_test()
{
	char k1[20]={0},k2[20]={0};
	char v1[100]={0},v2[20]={0},v3[20]={0};

	//init test
	db_init(1024*1024,0);

	sprintf(k1,"%s","key1");
	sprintf(v1,"%s","value1");

	sprintf(k2,"%s","key2");
	sprintf(v2,"%s","value2");

	//add test
	db_add(k1,v1);
	db_add(k2,v2);

	void* data=db_get(k1);
	assert(strcmp(data,v1)==0);
	free(data);

	data=db_get(k2);
	assert(strcmp(data,v2)==0);
	free(data);
	LOG("%s PASSED....","db get");

	//remove test
	db_remove(k2);
	data=db_get(k2);
	assert(data==NULL);
	db_destroy();
	LOG("%s PASSED....","db remove");
}

int main()
{
	//bloom filter unit-test
	test_bloom_init();
	test_bloom_add();
	test_bloom_get();

	//hashtable unit-test
	test_ht_int();
	test_ht_string();

	//database unit-test
	 test_db_test();
	 LOG("ALL PASSED....%s\n","");

	exit(0);
}
