#ifndef _I_SPEAK_DB
#define _I_SPEAK_DB

#include <stddef.h>

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

#endif
