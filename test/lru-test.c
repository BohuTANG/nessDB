#include "../engine/lru.h"
#include "../engine/debug.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

void _dump_list(void *node)
{
	struct list_node *n = (struct list_node*)node;

	if (n)
		__DEBUG("---key:%s, used<bytes>:%d", n->sk.data, n->size);
}

int main()
{
	char key[16];
	char val[16];
	struct slice sk, sv;
	struct lru *lru = lru_new(180);

	int i;
	for (i = 0; i < 1000000; i++) {
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
		free(sv.data);
	} else 
		__INFO("not get lru:%s", sk.data);

	lru_remove(lru, &sk);

	ret = lru_get(lru, &sk, &sv);
	if (ret) {
		__INFO("get from lru:%s, val:%s", sk.data, sv.data);
		free(sv.data);
	} else 
		__INFO("not get lru:%s", sk.data);

	__INFO("------lru count:%llu, allow:%llu, used:%llu", lru->list->count, lru->allow, lru->list->used);

	memset(key, 0, 16);
	snprintf(key, 16, "ky%d", i);
	sk.len = strlen(key);
	sk.data = key;

	list_reverse(lru->list, _dump_list);
	lru_remove(lru, &sk);
	lru_free(lru);

	return 0;
}
