#ifndef __CONFIG_H
#define __CONFIG_H

#define NESSDB_MAX_KEY_SIZE (24) /* max key size */
#define MTBL_MAX_COUNT (6000000) /* max count in memtable */
#define SST_MAX_COUNT (65536) /* one sst's max count entries */
#define META_MAX_COUNT (10000) /* max meta count */
#define BLOOM_BITS (433494437) /* bloom filter bits */
#define FILE_PATH_SIZE (1024) /* file full path */
#define FILE_NAME_SIZE (256) /* file name size */

#endif
