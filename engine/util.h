#ifndef _UTIL_H
#define _UTIL_H

#include "platform.h"

struct slice{
	char *data;
	int len;
};

void _ensure_dir_exists(const char *path);
const char *concat_paths(const char *basedir, const char *subdir);

int _file_exists(const char *path);
UINT _getsize(int fd);

#endif
