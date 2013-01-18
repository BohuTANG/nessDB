#ifndef __nessDB_META_H
#define __nessDB_META_H

#include "internal.h"
#include "db.h"
#include "sst.h"

#define NESSDB_MAX_META (3000) 
#define META_NODE_SIZE sizeof(struct meta_node)

struct meta_node {
	int lsn;
	struct sst *sst;
};

struct meta {
	int size;
	char path[NESSDB_PATH_SIZE];
	char sst_file[NESSDB_PATH_SIZE];
	struct meta_node nodes[NESSDB_MAX_META];
	struct stats *stats;
};

struct meta *meta_new(const char *path, struct stats *stats);
struct meta_node *meta_get(struct meta *meta, char *key);
struct meta_node *meta_scan(struct meta *meta, char *start, char *end,
							 int *ret_c);
void meta_dump(struct meta *meta);
void meta_free(struct meta *meta);

#endif
