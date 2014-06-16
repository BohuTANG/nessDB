/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_FILE_H_
#define nessDB_FILE_H_

#include "internal.h"

void ness_set_fun_open(int (*open_fun)(const char *, int, int));
void ness_set_fun_pwrite(int (*pwrite_fun)(int, const void*, size_t, DISKOFF));
void ness_set_fun_pread(int (*pread_fun)(int, void*, size_t, DISKOFF));
void ness_set_fun_close(int (*close_fun)(int));

int ness_os_open(const char *path, int oflag, int mode);
int ness_os_open_direct(const char *path, int oflag, int mode);
ssize_t ness_os_pwrite(int fd, const void *buf, size_t len, DISKOFF off);
ssize_t ness_os_pread(int fd, void *buf, size_t len, DISKOFF off);

ssize_t ness_os_write(int fd, const void *buf, size_t len);
ssize_t ness_os_read(int fd, void *buf, size_t len);

int ness_os_fsync(int fd);
int ness_os_close(int fd);

void ness_check_dir(const char *pathname);
int ness_file_exist(const char *filename);

#endif /* nessDB_FILE_H_ */
