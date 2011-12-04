#ifndef _DB_ZMQ_H
#define _DB_ZMQ_H

#include "../i_speak_db.h"

void* dbz_load(const char *filename);
struct dbz_op* dbz_op(void* _ctx, const char* name);
//struct dbz_op* dbz_bind(void* _ctx, const char *name, const char *addr);
int dbz_close(void* _ctx);

#endif
