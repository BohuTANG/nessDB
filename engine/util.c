#ifndef _GNU_SOURCE
	#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

const char *concat_paths(const char *basedir, const char *subdir)
{
	char *path=calloc(1,sizeof(char)*256);
	snprintf(path, sizeof(path), "%s/%s", basedir, subdir);
	return path;
}

static void _portable_mkdir(const char *path)
{
#ifdef _WIN32
	_mkdir(path);
#else
	mkdir(path, S_IRWXU | S_IRGRP | S_IROTH);
#endif
}

static void _mkdirs(const char *path)
{
	char tmp[256] = {0};
	char *p = NULL;
	size_t len;
	snprintf(tmp, sizeof(tmp), "%s", path);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') {
		tmp[len - 1] = 0;
	}
	for(p = tmp +1; *p; p++) {
		if (*p == '/') {
			*p = 0;
			_portable_mkdir(tmp);
			*p = '/';
		}
	}
	_portable_mkdir(tmp);
}

void _ensure_dir_exists(const char *path)
{
	struct stat st;
	if(stat(path, &st) != 0) {
		_mkdirs(path);
	}
}
