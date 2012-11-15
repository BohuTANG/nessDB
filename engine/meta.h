#ifndef _META_H
#define _META_H

#include "config.h"
#include "cola.h"
#include "compact.h"

#define META_NODE_SIZE sizeof(struct meta_node)
#define NESSDB_MAX_META (3000) 

struct meta_node{
	int lsn;
	struct cola *cola;
};

struct meta{
	int size;
	char path[NESSDB_PATH_SIZE];
	char sst_file[NESSDB_PATH_SIZE];
	struct meta_node nodes[NESSDB_MAX_META];
	struct compact *cpt;
};

struct meta *meta_new(const char *path);
struct meta_node *meta_get(struct meta *meta, char *key);
void meta_set(struct meta *meta, struct meta_node *node);
void meta_set_byname(struct meta *meta, struct meta_node *node);
void meta_dump(struct meta *meta);
void meta_free(struct meta *meta);

#endif
