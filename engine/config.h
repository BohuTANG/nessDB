#ifndef __CONFIG_H
#define __CONFIG_H

/* 
 * max key size 
 */
#define NESSDB_MAX_KEY_SIZE (24) 		

/* 
 * max cgroups,if 0 no cgroups
 * if confirm never can change
 * the recommend default is 1
 */
#define NESSDB_MAX_CGROUPS (1)			

/*
 * max count in memtable 
 */
#define MTBL_MAX_COUNT (6000000 / NESSDB_MAX_CGROUPS)			

/* 
 * one sst's max count entries
 */
#define SST_MAX_COUNT (65536)		

/* 
 * max meta count 
 */
#define META_MAX_COUNT (10000 / NESSDB_MAX_CGROUPS)		

/* 
 * bloom filter bits 
 */
#define BLOOM_BITS (433494437 / NESSDB_MAX_CGROUPS)			

/*
 * file full path  
 */
#define FILE_PATH_SIZE (1024)		

/*
 * file name size 
 */
#define FILE_NAME_SIZE (256)			

#endif
