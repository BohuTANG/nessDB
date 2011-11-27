#ifndef _UTIL_H
#define _UTIL_H

#include <stdint.h>

struct slice{
	char *data;
	int len;
};

void _ensure_dir_exists(const char *path);
const char *concat_paths(const char *basedir, const char *subdir);

int _file_exists(const char *path);
uint64_t _getsize(int fd);

#endif
