#include "db-zmq.h"

#include "../engine/db.h"
#include "sha1.h"

#include <unistd.h>
#include <assert.h>

struct nessdb* db = NULL;

static struct nessdb* open_db(void *can_reply, void *reply_directed) {
	// The values of can_reply and reply_directed represent the output
	// channel state.
	// can_reply && reply_directed = REPLY/PAIR socket
	// can_reply && !reply_directed = PUB socket
	// !can_reply = PULL/SUB socket
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

	// The key should be unique to the database
	// ∃! k ∈ DB: SHA1(k)
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

void*
i_speak_db(void){
	static struct dbz_op ops[] = {
		{"put", 0, (dbzop_t)nessdb_put, NULL},
		{"get", 1, (dbzop_t)nessdb_get, NULL},
		{"del", 0, (dbzop_t)nessdb_del, NULL},
		//{"next", 0, (dbzop_t)nessdb_next, NULL},
		{NULL, 0, 0, 0}
	};
	return &ops;
}