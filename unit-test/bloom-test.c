#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "../src/bloom.h"
#include "ct/ct.h"

void cttest_bloom_init()
{
	struct bloom _b;
	bloom_init(&_b,1000);
	assert(0 == 0);
}

void cttest_bloom_add()
{
	const char *k="yes";
	struct bloom _b;
	bloom_init(&_b,1000);
	bloom_add(&_b,k);
	int ret=bloom_get(&_b,k);
	assert(ret == 0);
}

void cttest_bloom_get()
{
	const char *k="yes";
	struct bloom _b;
	bloom_init(&_b,1000);
	bloom_add(&_b,k);
	int ret=bloom_get(&_b,"no");
	assert(ret ==-1);
}

