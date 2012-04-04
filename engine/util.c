/*
 * nessDB storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

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
#include <sys/time.h>
#include "util.h"

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

void ensure_dir_exists(const char *path)
{
	struct stat st;

	if(stat(path, &st) != 0) {
		_mkdirs(path);
	}
}

/**
 * SAX hash function
 */
unsigned int sax_hash(const char *key)
{
	unsigned int h = 0;

	while (*key) {
		h ^= (h << 5) + (h >> 2) + (unsigned char) *key;
		++key;
	}
	return h;
}

/**
 * SDBM hash function
 */
unsigned int sdbm_hash(const char *key)
{
	unsigned int h = 0;

	while (*key) {
		h = (unsigned char) *key + (h << 6) + (h << 16) - h;
		++key;
	}
	return h;
}

/**
 * Jdb hash function
 */
unsigned int djb_hash(const char *key)
{
	unsigned int h = 5381;

	while (*key) {
		h = ((h<< 5) + h) + (unsigned int) *key;  /* hash * 33 + c */
		++key;
	}

	return h;
}

long long get_ustime_sec(void)
{
	struct timeval tv;
	long long ust;

	gettimeofday(&tv, NULL);
	ust = ((long long)tv.tv_sec)*1000000;
	ust += tv.tv_usec;
	return ust / 1000000;
}


