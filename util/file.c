/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

int ness_os_open(const char *path, int oflag, int mode)
{
	int r;

	r = open(path, oflag, mode);

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
int ness_os_pwrite(int fd, const void *buf, size_t len, uint64_t off)
{
	int result = 0;
	const char *bp = (char*)buf;

	nassert((long long) bp % ALIGNMENT == 0);
	nassert((long long) len % ALIGNMENT == 0);
	nassert((long long) off % ALIGNMENT == 0);

	while (len > 0) {
		ssize_t written;

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

	ssize_t read = pread(fd, buf, len, off);

	return read;
}


/*
 * RETURN:
 *	0 SUCCESS
 *	errono
 */
int ness_os_write(int fd, const void *buf, size_t len)
{
	int result = 0;
	const char *bp = (const char *) buf;

	while (len > 0) {
		ssize_t written;

		written = write(fd, bp, len);
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

	reads = read(fd, buf, count);

	return reads;
}

int ness_os_fsync(int fd)
{
	int r = -1;

	r = fsync(fd);

	return r;
}

int ness_os_close(int fd)
{
	int r = -1;

	r = close(fd);

	return r;
}

struct ness_vfs ness_osvfs = {
	.open	= &ness_os_open,
	.pwrite	= &ness_os_pwrite,
	.pread	= &ness_os_pread,
	.write	= &ness_os_write,
	.read	= &ness_os_read,
	.fsync	= &ness_os_fsync,
	.close	= &ness_os_close,
};
