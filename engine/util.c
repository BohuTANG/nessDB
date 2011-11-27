#ifndef _GNU_SOURCE
	#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "util.h"

const char *concat_paths(const char *basedir, const char *subdir)
{
	char *path = malloc(256);
	memset(path, 0, 256);
	snprintf(path, 256, "%s/%s", basedir, subdir);
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

int _file_exists(const char *path)
{
	int fd = open(path, O_RDWR);
	if (fd > -1) {
		close(fd);
		return 1;
	}
	return 0;
}

uint64_t _getsize(int fd) {
	struct stat sb;
	if (fstat(fd,&sb) == -1)
		return 0;

	return (uint64_t)sb.st_size;
}
