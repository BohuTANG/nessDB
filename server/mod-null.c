#include "db-zmq.h"

#define IGNORE_ARGS (void)in_data;(void)in_sz;(void)cb;(void)token;

DB_OP(nullop_put){
	IGNORE_ARGS;
	return 0;
}

DB_OP(nullop_get){
	IGNORE_ARGS;
	cb(in_data, in_sz, NULL, token);
	return 0;
}

DB_OP(nullop_del){
	IGNORE_ARGS;
	return 0;
}

void*
i_speak_db(void){
	static struct module_feature ops[] = {
		{"put", 0, (dbzop_t)nullop_put, NULL},
		{"get", 1, (dbzop_t)nullop_get, NULL},
		{"del", 0, (dbzop_t)nullop_del, NULL},
		{NULL, 0, 0, 0}
	};
	return &ops;
}