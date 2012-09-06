#include "../engine/lru.h"
#include "../engine/debug.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

int main()
{
	char key[16];
	char val[16];
	struct slice sk, sv;
	struct lru *lru = lru_new(100);

	int i;
	for (i = 0; i < 100000; i++) {
		memset(key, 0, 16);
		snprintf(key, 16, "key%d", i);

		memset(val, 0, 16);
		snprintf(val, 16, "val%d", i);

		sk.len = strlen(key);
		sk.data = key;

		sv.len = strlen(val);
		sv.data = val;
		lru_set(lru, &sk, &sv);
	}

	int ret = lru_get(lru, &sk, &sv);
	if (ret) {
		__INFO("get from lru:%s, val:%s", sk.data, sv.data);
	} else 
		__INFO("not get lru:%s", sk.data);

	lru_remove(lru, &sk);

	ret = lru_get(lru, &sk, &sv);
	if (ret) {
		__INFO("get from lru:%s, val:%s", sk.data, sv.data);
	} else 
		__INFO("not get lru:%s", sk.data);

	__INFO("------lru count:%lu, used:%lu", lru->count, lru->list->used_size);

	memset(key, 0, 16);
	snprintf(key, 16, "ky%d", i);
	sk.len = strlen(key);
	sk.data = key;

	lru_remove(lru, &sk);
	lru_free(lru);

	return 0;
}
