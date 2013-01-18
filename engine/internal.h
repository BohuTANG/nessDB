#ifndef __nessDB_INTERNAL_H
#define __nessDB_INTERNAL_H

#define _BSD_SOURCE

#if defined(__linux__)
#define _GNU_SOURCE
#endif

#if defined(__linux__) || defined(__OpenBSD__) || defined(__NetBSD__)
#define _XOPEN_SOURCE 700
#else
#define _XOPEN_SOURCE
#endif

#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#ifndef O_BINARY
#define O_BINARY (0) 
#endif

#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <stdint.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <execinfo.h>
#include <signal.h>
#include <ucontext.h>
#include <time.h>
#include <sys/time.h>
#include <stdarg.h>
#include <errno.h>


#if defined(__linux__)
	# define n_open (open64)
	# define n_lseek (lseek64)
	# define n_fstat (fstat64)
	# define n_pwrite (pwrite64)
	# define N_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY | O_LARGEFILE)
	# define N_OPEN_FLAGS   (O_RDWR | O_BINARY | O_LARGEFILE)
#else
	# define n_open (open)
	# define n_lseek (lseek)
	# define n_fstat (fstat)
	# define n_pwrite (pwrite)
	# define N_CREAT_FLAGS  (O_RDWR | O_TRUNC | O_CREAT | O_BINARY)
	# define N_OPEN_FLAGS   (O_RDWR | O_BINARY)
#endif

#define NESSDB_VERSION ("v2.0")               /* nessDB version flag   */
#define NESSDB_SST_SEGMENT (4)                /* SST splited numbers   */
#define NESSDB_PATH_SIZE (1024)               /* Max length of path    */ 
#define NESSDB_MAX_VAL_SIZE (1024*1024*10)    /* Max value length      */

#define NESSDB_COMPRESS_LIMIT (1024)          /* Flag of compression   */
#define NESSDB_COMPACT_LIMIT (1000)           /* Flag of compact       */

/* NOTICE: These macros can NOT be modified
 * OR you will be lost in ness
 */
#define MAX_LEVEL (4)
#define LEVEL_BASE (4)
#define L0_SIZE (256*1024)
#define BLOCK_GAP (256)
#define NESSDB_MAX_KEY_SIZE (36) 

#define NESSDB_SST_EXT (".SST")
#define NESSDB_DB ("ness.DB")

struct sst_item {
	char data[NESSDB_MAX_KEY_SIZE];
	uint64_t offset;
	uint32_t vlen:30;
	int opt:1;
	int in:1;
} __attribute__((packed));

struct stats {
	unsigned long long STATS_WRITES;              /* all counts #write            */
	unsigned long long STATS_READS;               /* all counts #read             */
	unsigned long long STATS_REMOVES;             /* all counts #remove           */
	unsigned long long STATS_R_FROM_MTBL;         /* all counts #mtbl */
	unsigned long long STATS_R_COLA;              /* all counts #read filesystem  */
	unsigned long long STATS_R_NOTIN_COLA;        /* all counts #read filesystem  */
	unsigned long long STATS_R_BF;                /* all counts #not bloomfilter  */
	unsigned long long STATS_R_NOTIN_BF;          /* all counts #not bloomfilter  */
	unsigned long long STATS_CRC_ERRS;            /* all crc errors               */
	unsigned long long STATS_COMPRESSES;          /* all counts #compress         */

	unsigned long long STATS_LEVEL_MERGES;        /* all counts #levels merge     */
	unsigned long long STATS_SST_SPLITS;          /* all counts #SST split        */
	unsigned long long STATS_SST_MERGEONE;        /* all counts #SST merge to one */
	time_t   STATS_START_TIME;			
	double STATS_DB_WASTED;
};

#endif
