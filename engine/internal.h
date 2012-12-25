#ifndef __nessDB_INTERNAL_H
#define __nessDB_INTERNAL_H

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
#include <math.h>
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
#define NESSDB_MAX_KEY_SIZE (48) 

#define NESSDB_SST_EXT (".SST")
#define NESSDB_DB ("ness.DB")

struct sst_item {
	char data[NESSDB_MAX_KEY_SIZE];
	uint64_t offset;
	uint32_t vlen;
	char opt;
} __attribute__((packed));

struct stats {
	volatile unsigned long long STATS_WRITES;              /* all counts #write            */
	volatile unsigned long long STATS_READS;               /* all counts #read             */
	volatile unsigned long long STATS_REMOVES;             /* all counts #remove           */
	volatile unsigned long long STATS_MERGES;              /* all counts #background merge */

	volatile unsigned long long STATS_R_FROM_MTBL;         /* all counts #mtbl */
	volatile unsigned long long STATS_R_COLA;              /* all counts #read filesystem  */
	volatile unsigned long long STATS_R_NOTIN_COLA;        /* all counts #read filesystem  */
	volatile unsigned long long STATS_R_BF;                /* all counts #not bloomfilter  */
	volatile unsigned long long STATS_R_NOTIN_BF;          /* all counts #not bloomfilter  */
	volatile unsigned long long STATS_CRC_ERRS;            /* all crc errors               */
	volatile unsigned long long STATS_COMPRESSES;          /* all counts #compress         */
	volatile unsigned long long STATS_HOLE_REUSES;         /* all counts #holes reuse      */

	volatile unsigned long long STATS_LEVEL_MERGES;        /* all counts #levels merge     */
	volatile unsigned long long STATS_SST_SPLITS;          /* all counts #SST split        */
	volatile unsigned long long STATS_SST_MERGEONE;        /* all counts #SST merge to one */
	volatile time_t   STATS_START_TIME;			
};

#endif
