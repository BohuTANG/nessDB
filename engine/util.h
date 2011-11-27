#ifndef _UTIL_H
#define _UTIL_H

#include "platform.h"

struct slice{
	char *data;
	size_t len;
};
typedef struct slice slice_t;

void _ensure_dir_exists(const char *path);
const char *concat_paths(const char *basedir, const char *subdir);

int _file_exists(const char *path);
UINT _getsize(int fd);

#endif
