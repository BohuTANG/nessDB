#include "db-zmq.h"

#include "../engine/db.h"
#include "sha1.h"

#include <unistd.h>
#include <assert.h>

struct nessdb* db = NULL;

static struct nessdb* open_db(void *can_reply, void *reply_directed) {
	// TODO: use these later
	(void)can_reply;
	(void)reply_directed;
	if( ! db ) {
		db = db_open(4 * 1024 * 1024, getcwd(NULL,0));
		assert( db != NULL );
	}
	return db;
}

DB_OP(nessdb_put){
	open_db(cb, token);
	struct slice sk = {NULL, 0, 0};
	struct slice sv = {NULL, 0, 0};
	sha1nfo data_hash;

	// Unique Key = SHA1(data)
	sha1_init(&data_hash);
	sha1_write(&data_hash, in_data, in_sz);
	sk.data = (char*)sha1_result(&data_hash);
	sk.len = HASH_LENGTH;

	db_add(db, &sk, &sv);
	return sk.len + sv.len;
}

DB_OP(nessdb_get){
	open_db(cb, token);
	struct slice sk = {in_data, in_sz, 0};
	struct slice sv = {NULL, 0, 0};

	if( db_get(db, &sk, &sv) ) {
		cb(sv.data, sv.len, NULL, token);
	}
	return sk.len + sv.len;
}

DB_OP(nessdb_del){
	open_db(cb, token);

	struct slice sk = {in_data, in_sz, 0};
	db_remove(db, &sk);
	return sk.len;
}

DB_OP(nessdb_idx){
	if( in_sz < 21 ) {
		return 0;
	}

	struct slice sv = {in_data, 20, 0};
	struct slice sk = {in_data + 20, in_sz - 20, 0};

	open_db(cb, token);
	// TODO: put in tree
	return sk.len + sv.len;
}

void*
i_speak_db(void){
	static struct module_feature ops[] = {
		{"put", 0, (dbzop_t)nessdb_put, NULL},
		{"get", 1, (dbzop_t)nessdb_get, NULL},
		{"del", 0, (dbzop_t)nessdb_del, NULL},
		{"idx", 0, (dbzop_t)nessdb_idx, NULL},
		{NULL, 0, 0, 0}
	};
	return &ops;
}