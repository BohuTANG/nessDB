/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_VFS_H_
#define nessDB_VFS_H_

/*
 * align to ALIGNMENT for direct I/O
 * but it is not always 512 bytes:
 * http://people.redhat.com/msnitzer/docs/io-limits.txt
 */
#define ALIGNMENT (512)
typedef uint64_t DISKOFF;
static inline uint64_t ALIGN(uint64_t v)
{
	return (v + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
}

struct ness_vfs {
	int (*open)(const char *path, int oflag, int mode);
	ssize_t (*read)(int fd, void *buf, size_t len);
	ssize_t (*pread)(int fd, void *buf, size_t len, uint64_t off);
	int (*write)(int fd, const void *buf, size_t len);
	int (*pwrite)(int fd, const void *buf, size_t len, uint64_t off);
	int (*fsync)(int fd);
	int (*close)(int fd);
} NESSPACKED;

int ness_file_exist(const char *filename);
void ness_check_dir(const char *pathname);

#endif /* nessDB_VFS_H_ */
