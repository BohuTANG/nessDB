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

size_t
example_put(
	char* in_data,
	size_t in_sz,
	module_callback cb,
	void* token
){
	open_db(cb, token);
	struct slice sk = {NULL, 0, 0};
	struct slice sv = {NULL, 0, 0};
	sha1nfo data_hash;

	// Key = SHA1(data)
	sha1_init(&data_hash);
	sha1_write(&data_hash, in_data, in_sz);
	sk.data = (char*)sha1_result(&data_hash);
	sk.len = HASH_LENGTH;

	db_add(db, &sk, &sv);
	return sk.len + sv.len;
}

size_t
example_get(
	char* in_data,
	size_t in_sz,
	module_callback cb,
	void* token
){
	open_db(cb, token);
	struct slice sk = {in_data, in_sz, 0};
	struct slice sv = {NULL, 0, 0};

	if( db_get(db, &sk, &sv) ) {
		cb(sv.data, sv.len, NULL, token);
	}
	return sk.len + sv.len;
}

size_t
example_del(
	char* in_data,
	size_t in_sz,
	module_callback cb,
	void* token
){
	open_db(cb, token);

	struct slice sk = {in_data, in_sz, 0};
	db_remove(db, &sk);
	return sk.len;
}

void*
i_speak_db(void){
	static struct module_feature ops[] = {
		{"put", 0, (module_callback)example_put, NULL},
		{"get", 1, (module_callback)example_get, NULL},
		{"del", 0, (module_callback)example_del, NULL},
		{NULL, 0, 0, 0}
	};
	return &ops;
}