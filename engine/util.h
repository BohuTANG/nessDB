#ifndef _UTIL_H
#define _UTIL_H

#include <stdint.h>

struct slice{
	char *data;
	int len;
	uint64_t park;	// XXX: what is this for?
};

void _ensure_dir_exists(const char *path);
const char *concat_paths(const char *basedir, const char *subdir);

#endif
