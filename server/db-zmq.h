#ifndef _DB_ZMQ_H
#define _DB_ZMQ_H

#include <stddef.h>

//typedef size_t (*send_cb)(void *owner, char* data, size_t len);

typedef size_t (*dbzop_t)(
	const char* in_data,
	size_t in_sz,
	void* cb,
	void* token
);

struct dbz_op {	
	const char* name;
	size_t opts;
	dbzop_t cb;
	void* token;
};

typedef struct dbz_op* (*mod_init_fn)();

#define DB_OP(name) size_t name ( char* in_data, size_t in_sz, dbzop_t cb, void* token )

void* dbz_load(const char *filename);
dbzop_t dbz_bind_op(void* _ctx, const char* name);
struct dbz_op* dbz_bind(void* _ctx, const char *name, const char *addr);
int dbz_close(void* _ctx);

#endif
