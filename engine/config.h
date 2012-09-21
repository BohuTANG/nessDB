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

#if defined(__linux__)
	# define n_open (open64)
	# define n_lseek (lseek64)
	# define n_fstat (fstat64)
	# define LSM_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE)
	# define LSM_OPEN_FLAGS   (O_RDWR | O_BINARY | O_LARGEFILE)
#else
	# define n_open (open)
	# define n_lseek (lseek)
	# define n_fstat (fstat)
	# define LSM_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY)
	# define LSM_OPEN_FLAGS   (O_RDWR | O_BINARY)
#endif

#define NESSDB_MAX_KEY_SIZE (64) /* max key size */
#define NESSDB_MAX_VAL_SIZE (4 * 1024 * 1024) /* max value size,4Mb */
#define LOG_BUFFER_SIZE (1024*200) /* log buffer size*/
#define MTBL_MAX_COUNT (2000000) /* max count in memtable */
#define SST_MAX_COUNT (65536) /* one sst's max count entries */
#define META_MAX_COUNT (10000) /* max meta count */
#define BLOOM_BITS (32454713) /* bloom filter bits :http://www.bigprimes.net/archive/prime/20001/ */
#define FILE_PATH_SIZE (1024) /* file full path */
#define FILE_NAME_SIZE (256) /* file name size */
#define DB_VERSION ("1.8.1")
#define DB_NAME ("ness.db")

#endif
