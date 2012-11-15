#ifndef __CONFIG_H
#define __CONFIG_H

#ifndef _GNU_SOURCE
	#define _GNU_SOURCE
#endif

#ifndef __USE_FILE_OFFSET64
	#define __USE_FILE_OFFSET64
#endif

#ifndef __USE_LARGEFILE64
	#define __USE_LARGEFILE64
#endif

#ifndef _LARGEFILE64_SOURCE
	#define _LARGEFILE64_SOURCE
#endif

#ifndef O_BINARY
	#define O_BINARY (0) 
#endif

#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>

#if defined(__linux__)
	# define n_open (open64)
	# define n_lseek (lseek64)
	# define n_fstat (fstat64)
	# define N_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE)
	# define N_OPEN_FLAGS   (O_RDWR | O_BINARY | O_LARGEFILE)
#else
	# define n_open (open)
	# define n_lseek (lseek)
	# define n_fstat (fstat)
	# define N_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY)
	# define N_OPEN_FLAGS   (O_RDWR | O_BINARY)
#endif

#define NESSDB_IS_LOG_RECOVERY (1)
#define NESSDB_SST_SEGMENT (4)
#define NESSDB_PATH_SIZE (1024) 
#define NESSDB_MAX_MTB_SIZE (500000)
#define NESSDB_MAX_VAL_SIZE (1024*1024*10)
#define NESSDB_COMPRESS_LIMIT (1024)
#define NESSDB_COMPACT_LIMIT (1000)

#define NESSDB_LOG_EXT (".LOG")
#define NESSDB_SST_EXT (".SST")
#define NESSDB_DB ("ness.DB")

struct slice {
	char *data;
	int len;
};

typedef enum {nERR = 0, nOK = 1} STATUS;

#endif
