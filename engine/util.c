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

int _creatdir(const char *pathname)  
{  
	int i, len;
	char  dirname[256];  

	strcpy(dirname, pathname);  
	i = strlen(dirname);  
	len = i;

	if (dirname[len-1] != '/')  
		strcat(dirname,  "/");  

	len = strlen(dirname);  
	for (i = 1; i < len; i++) {  
		if (dirname[i] == '/') {  
			dirname[i] = 0;  
			if(access(dirname, 0) != 0) {  
				if(mkdir(dirname,   0755)==-1) {   
					return -1;  
				}  
			}  
			dirname[i] = '/';  
		}  
	}  

	return 0;  
} 

void ensure_dir_exists(const char *path)
{
	if (_creatdir(path) != 0)
		perror("mkdir floder error");
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

