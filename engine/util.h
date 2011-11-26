#ifndef _UTIL_H
#define _UTIL_H

struct slice{
	char *data;
	int len;
};

void _ensure_dir_exists(const char *path);
const char *concat_paths(const char *basedir, const char *subdir);

#endif
