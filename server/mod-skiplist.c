#include "db-zmq.h"

#include "../engine/skiplist.h"

#include <unistd.h>
#include <assert.h>

struct skiplist* db = NULL;

static struct skiplist* open_db(void *can_reply, void *reply_directed) {
	(void)can_reply;
	(void)reply_directed;
	if( ! db ) {
		db = skiplist_new(1024 * 1024);
		assert( db != NULL );
	}
	return db;
}

DB_OP(skiplist_put){
	open_db(cb, token);
	struct slice sk = {in_data, in_sz, 0};
	size_t val = sk.len;
	if( skiplist_insert(db, &sk, val, ADD) ) {
		val += sizeof(struct skipnode);
	}

	return val;
}

DB_OP(skiplist_get){
	open_db(cb, token);
	struct slice sk = {in_data, in_sz, 0};
	struct skipnode *n;
	size_t valid_len = in_sz + sizeof(struct skipnode);

	if( (n = skiplist_lookup(db, &sk)) ) {
		if( n->val == valid_len ) {
			cb((char*)n, sizeof(struct skipnode), NULL, token);
			return valid_len;
		}
		return sk.len;
	}
	return 0;
}

DB_OP(skiplist_del){
	open_db(cb, token);
	struct slice sk = {in_data, in_sz, 0};
	size_t val = sk.len;
	if( skiplist_insert(db, &sk, val, DEL) ) {
		val += sizeof(struct skipnode);
	}

	return val;
}

void*
i_speak_db(void){
	static struct module_feature ops[] = {
		{"put", 0, (dbzop_t)skiplist_put, NULL},
		{"get", 1, (dbzop_t)skiplist_get, NULL},
		{"del", 0, (dbzop_t)skiplist_del, NULL},
		//{"next", 0, (dbzop_t)nessdb_next, NULL},
		{NULL, 0, 0, 0}
	};
	return &ops;
}