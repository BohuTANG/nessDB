#include "db-zmq.h"

#include "../engine/ht.h"

#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <stdbool.h>
#include <err.h>
#include <stdlib.h>

static bool db_ready = false;
static struct ht db;

struct my_node {	
	struct ht_node node;
	uint32_t sz;
	char k[4];
};

static size_t
my_hashfunc(void *_n) {
	unsigned hash = 5318;
	struct my_node* n = (struct my_node*)_n;
	for( int i = 0; i < 4; i++ ) {
		hash = ((hash << 5) + hash) + (unsigned int)n->k[i];
	}
	return hash;
}

static int
my_cmpfunc(void *_a, void *_b) {
	struct my_node* a = (struct my_node*)_a;
	struct my_node* b = (struct my_node*)_b;
	return memcmp(a->k, b->k, sizeof(uint32_t));
}

static struct ht* open_db(void *can_reply, void *reply_directed) {
	(void)can_reply;
	(void)reply_directed;
	if( ! db_ready ) {
		ht_init(&db, 1024 * 1024);
		db.hashfunc = my_hashfunc;
		db.cmpfunc = my_cmpfunc;
		db_ready = true;
	}
	return &db;
}

DB_OP(htop_put){
	if( in_sz < 5 ) {
		warnx("Must operate on 20b-key + N size value");
		return 0;
	}

	struct my_node* node = malloc(sizeof(struct my_node) + (in_sz - sizeof(uint32_t)));
	memcpy(node->k, in_data, in_sz);
	node->sz = in_sz;

	open_db(cb, token);
	ht_set(&db, (struct ht_node*)node);
	return in_sz;
}

DB_OP(htop_get){
	if( in_sz != 4 ) {
		return 0;
	}

	open_db(cb, token);
	struct my_node to_find = {{NULL}, 0, {in_data[0],in_data[1],in_data[2],in_data[3]}};
	struct my_node* n = (struct my_node*)ht_get(&db, &to_find);

	if( ! n ) {
		return in_sz;
	}

	cb(&n->k[0], n->sz, NULL, token);
	return in_sz + n->sz;
}

DB_OP(htop_del){
	if( in_sz != 4 ) {
		return 0;
	}

	open_db(cb, token);
	struct my_node to_find = {{NULL}, 0, {in_data[0],in_data[1],in_data[2],in_data[3]}};
	struct my_node* n = (struct my_node*)ht_remove(&db, &to_find);
	size_t val = in_sz;
	if( n ) {
		val += n->sz;
		free(n);
	}

	return val;
}

void*
i_speak_db(void){
	static struct module_feature ops[] = {
		{"put", 0, (dbzop_t)htop_put, NULL},
		{"get", 1, (dbzop_t)htop_get, NULL},
		{"del", 0, (dbzop_t)htop_del, NULL},
		//{"next", 0, (dbzop_t)nessdb_next, NULL},
		{NULL, 0, 0, 0}
	};
	return &ops;
}