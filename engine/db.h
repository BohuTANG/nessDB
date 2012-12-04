#ifndef __nessDB_H
#define __nessDB_H

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
#define NESSDB_MAX_MTB_SIZE (500000)          /* Max Memtable items    */
#define NESSDB_MAX_VAL_SIZE (1024*1024*10)    /* Max value length      */
#define NESSDB_COMPRESS_LIMIT (1024)          /* Flag of compression   */
#define NESSDB_COMPACT_LIMIT (1000)           /* Flag of compact       */
#define NESSDB_IS_LOG_RECOVERY (1)

/* NOTICE: These macros can NOT be modified, or you will be lost */
#define MAX_LEVEL (4)
#define LEVEL_BASE (8)
#define L0_SIZE (64*1024)
#define BLOCK_GAP (256)
#define NESSDB_MAX_KEY_SIZE (35) 

#define NESSDB_LOG_EXT (".LOG")
#define NESSDB_SST_EXT (".SST")
#define NESSDB_DB ("ness.DB")

typedef enum {nERR = 0, nOK = 1} STATUS;

struct slice {
	char *data;
	int len;
};

struct sst_item {
	char data[NESSDB_MAX_KEY_SIZE];
	uint64_t offset;
	uint32_t vlen;
	char opt;
} __attribute__((packed));

struct stats {
	volatile unsigned long long  STATS_WRITES;             /* all counts #write            */
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

	volatile unsigned long long STATS_MTBL_COUNTS;         /* all counts #MTBL             */
	volatile unsigned long long STATS_MTBL_MERGING_COUNTS; /* all counts #merging MTBL     */

	volatile unsigned long long STATS_LEVEL_MERGES;        /* all counts #levels merge     */
	volatile unsigned long long STATS_SST_SPLITS;          /* all counts #SST split        */
	volatile unsigned long long STATS_SST_MERGEONE;        /* all counts #SST merge to one */
	volatile time_t   STATS_START_TIME;			
};

#ifdef __cplusplus
extern "C" {
#endif

	struct nessdb {
		struct index *idx;
		struct stats *stats;
	};

	struct nessdb *db_open(const char *basedir);
	STATUS db_get(struct nessdb *db, struct slice *sk, struct slice *sv);
	STATUS db_exists(struct nessdb *db, struct slice *sk);
	STATUS db_add(struct nessdb *db, struct slice *sk, struct slice *sv);
	void db_stats(struct nessdb *db, struct slice *infos);
	void db_remove(struct nessdb *db, struct slice *sk);
	void db_close(struct nessdb *db);

#ifdef __cplusplus
}
#endif
#endif
