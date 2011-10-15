#ifndef _PLATFORM_H
#define _PLATFORM_H

#if defined(__linux__)
# define lseek lseek64
# define fstat fstat64
# define fsync fdatasync
# define BTREE_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE)
# define BTREE_OPEN_FLAGS   (O_RDWR | O_BINARY | O_LARGEFILE)
# define HAVE_CLOCK_GETTIME 1
#else
# define open64 open
# define BTREE_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY)
# define BTREE_OPEN_FLAGS   (O_RDWR | O_BINARY)
#endif

#endif

