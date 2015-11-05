/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_NESS_H_
#define nessDB_NESS_H_

#include <inttypes.h>
#ifdef __cplusplus
extern "C" {
#endif

	void *ness_env_open(const char *home, uint32_t flags);
	void ness_env_close(void *e);

	void *ness_db_open(void *e, const char *dbname);
	int ness_db_set(void *db, char *k, uint32_t ksize, char *v, uint32_t vsize);
	int ness_db_close(void *db);

	int ness_env_set_cache_size(void *nessenv, uint64_t cache_size);
	int ness_env_set_compress_method(void *nessenv, int method);

#ifdef __cplusplus
}
#endif

#endif /* nessDB_NESS_H_ */
