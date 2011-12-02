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

struct module_feature {	
	const char* name;
	size_t opts;
	dbzop_t cb;
	void* token;
};

typedef struct module_feature* (*mod_init_fn)();

#define DB_OP(name) size_t name ( char* in_data, size_t in_sz, dbzop_t cb, void* token )

#endif
