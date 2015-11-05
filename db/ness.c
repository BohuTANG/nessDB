/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "u.h"
#include "db.h"
#include "ness.h"

void *ness_env_open(const char *home, uint32_t flags)
{
	return env_open(home, flags);
}

void ness_env_close(void *nessenv)
{
	struct env *e = (struct env*)nessenv;
	env_close(e);
}

void *ness_db_open(void *nessenv, const char *dbname)
{
	struct env *e = (struct env*)nessenv;

	return db_open(e, dbname);
}

int ness_db_close(void *nessdb)
{
	struct nessdb *db = (struct nessdb*)nessdb;

	return db_close(db);
}

int ness_db_set(void *nessdb, char *k, uint32_t ksize, char *v, uint32_t vsize)
{
	struct msg kmsg = {.data = k, .size = ksize};
	struct msg vmsg = {.data = v, .size = vsize};

	struct nessdb *db = (struct nessdb*)nessdb;
	return db_set(db, &kmsg, &vmsg);
}

int ness_env_set_cache_size(void *nessenv, uint64_t cache_size)
{
	struct env *e = (struct env*)nessenv;

	return env_set_cache_size(e, cache_size);
}

int ness_env_set_compress_method(void *nessenv, int method)
{
	struct env *e = (struct env*)nessenv;

	return env_set_compress_method(e, method);
}

