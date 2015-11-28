/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

int ness_file_exist(const char *filename)
{
	if (access(filename, F_OK) != -1)
		return 1;
	else
		return 0;
}

void ness_check_dir(const char *pathname)
{
	int i;
	int len;
	char dirname[1024];

	memset(dirname, 0, 1024);
	strcpy(dirname, pathname);
	i = strlen(dirname);
	len = i;

	if (dirname[len - 1] != '/')
		strcat(dirname,  "/");

	len = strlen(dirname);
	for (i = 1; i < len; i++) {
		if (dirname[i] == '/') {
			dirname[i] = 0;
			if (access(dirname, 0) != 0) {
				if (mkdir(dirname, 0755) == -1)
					__PANIC("create dir error, %s", dirname);
			}
			dirname[i] = '/';
		}
	}
}


