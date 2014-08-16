/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "file.h"

static int (*n_open)(const char *path, int oflag, int mode);
static int (*n_pwrite)(int fd, const void *buf, size_t len, DISKOFF off);
static int (*n_pread)(int fd, void *buf, size_t len, DISKOFF off);
static int (*n_write)(int fd, const void *buf, size_t len);
static int (*n_read)(int fd, void *buf, size_t len);
static int (*n_fsync)(int fd);
static int (*n_close)(int fd);

void ness_set_fun_open(int (*open_fun)(const char *, int, int))
{
	n_open = open_fun;
}

void ness_set_fun_pwrite(int (*pwrite_fun)(int, const void*, size_t, DISKOFF))
{
	n_pwrite = pwrite_fun;
}

void ness_set_fun_pread(int (*pread_fun)(int, void*, size_t, DISKOFF))
{
	n_pread = pread_fun;
}

void ness_set_fun_write(int (*write_fun)(int, const void*, size_t))
{
	n_write = write_fun;
}

void ness_set_fun_read(int (*read_fun)(int, void*, size_t))
{
	n_read = read_fun;
}

void ness_set_fun_fsync(int (*fsync_fun)(int))
{
	n_fsync = fsync_fun;
}

void ness_set_fun_close(int (*close_fun)(int))
{
	n_close = close_fun;
}

int ness_os_open(const char *path, int oflag, int mode)
{
	int r;

	if (n_open)
		r = n_open(path, oflag, mode);
	else
		r = open(path, oflag, mode);

	return r;
}

int ness_os_open_direct(const char *path, int oflag, int mode)
{
	int r;

#if defined (__linux__)
#if ! defined(O_DIRECT)
#define O_DIRECT O_SYNC
#endif
	r = ness_os_open(path, oflag | O_DIRECT, mode);
#else
	r = ness_os_open(path, oflag, mode);
#endif

	return r;
}

static void _write_error_handler(int fd, size_t len)
{
	switch (errno) {
	case EINTR: { /* interrupted by a signal before any data was written; see signal(7) */
			/* 64 bit is 20 chars, 32 bit is 10 chars */
			char err_msg[sizeof("Write of [] bytes to fd=[] interrupted.  Retrying.") + 20 + 10];
			snprintf(err_msg,
			         sizeof(err_msg),
			         "Write of [%" PRIu64 "] bytes to fd=[%d] interrupted.  Retrying.",
			         (uint64_t)len,
			         fd);
			perror(err_msg);
			fflush(stderr);
			break;
		}
	case ENOSPC: {
			char err_msg[sizeof("Failed write of [] bytes to fd=[].") + 20 + 10];
			snprintf(err_msg,
			         sizeof(err_msg),
			         "Failed write of [%" PRIu64 "] bytes to fd=[%d].",
			         (uint64_t)len,
			         fd);
			perror(err_msg);
			fflush(stderr);
			break;
		}
	}
}

/*
 * REQUIRES:
 *	a) buf address must be aligned
 *	b) len must be aligned
 *	c) off must be aligned
 *
 * RETURN:
 *	0 SUCCESS
 *	errono
 */
ssize_t ness_os_pwrite(int fd, const void *buf, size_t len, DISKOFF off)
{
	ssize_t result = 0;
	const char *bp = (char*)buf;

	nassert((long long) bp % ALIGNMENT == 0);
	nassert((long long) len % ALIGNMENT == 0);
	nassert((long long) off % ALIGNMENT == 0);

	while (len > 0) {
		ssize_t written;

		if (n_pwrite)
			written = n_pwrite(fd, bp, len, off);
		else
			written = pwrite(fd, bp, len, off);

		if (written < 0) {
			result = errno;
			break;
		}

		len -= written;
		bp += written;
		off += written;
	}

	if (result != 0)
		_write_error_handler(fd, len);

	return result;
}

/*
 * REQUIRES:
 *	a) buf address must be aligned
 *	b) len must be aligned
 *	c) off must be aligned
 *
 * RETURN:
 *	0 SUCCESS
 *	errono
 */
ssize_t ness_os_pread(int fd, void *buf, size_t len, DISKOFF off)
{
	nassert((long long) buf % ALIGNMENT == 0);
	nassert((long long) len % ALIGNMENT == 0);
	nassert((long long) off % ALIGNMENT == 0);

	ssize_t read;

	if (n_pread)
		read = n_pread(fd, buf, len, off);
	else
		read = pread(fd, buf, len, off);

	return read;
}


/*
 * RETURN:
 *	0 SUCCESS
 *	errono
 */
ssize_t ness_os_write(int fd, const void *buf, size_t len)
{
	ssize_t result = 0;
	const char *bp = (const char *) buf;

	while (len > 0) {
		ssize_t written;

		if (n_write) {
			written = n_write(fd, bp, len);
		} else {
			written = write(fd, bp, len);
		}
		if (written < 0) {
			result = errno;
			break;
		}
		len -= written;
		bp += written;
	}

	if (result != 0)
		_write_error_handler(fd, len);

	return result;
}

/*
 * RETURN:
 *	read size
 */
ssize_t ness_os_read(int fd, void *buf, size_t count)
{
	ssize_t reads;

	if (n_read)
		reads = n_read(fd, buf, count);
	else
		reads = read(fd, buf, count);

	return reads;
}

int ness_os_fsync(int fd)
{
	int r = -1;

	if (n_fsync)
		r = n_fsync(fd);
	else
		r = fsync(fd);

	return r;
}

int ness_os_close(int fd)
{
	int r = -1;

	if (n_close)
		r = n_close(fd);
	else
		r = close(fd);

	return r;
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

int ness_file_exist(const char *filename)
{
	if (access(filename, F_OK) != -1)
		return 1;
	else
		return 0;
}
