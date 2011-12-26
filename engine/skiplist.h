/*
 * LSM-Tree storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#ifndef _SKIPLIST_H
#define _SKIPLIST_H

#include <stdint.h>

#define MAXLEVEL (15)
#define SKIP_KSIZE (32)

typedef enum {ADD,DEL} OPT;

struct skipnode{
    char key[SKIP_KSIZE];
	uint64_t  val;
	unsigned opt:24;                   
    struct skipnode *forward[1]; 
	struct skipnode *next;
};

struct skiplist{
	int count;
	int size;
	int level; 
	char pool_embedded[1024];
	struct  skipnode *hdr;                 
	struct pool *pool;
};

struct skiplist *skiplist_new(size_t size);
int skiplist_insert(struct skiplist *list, char *key, uint64_t val, OPT opt);
int skiplist_insert_node(struct skiplist *list, struct skipnode *node);
void skiplist_delete(struct skiplist *list, const char *data);
struct skipnode *skiplist_lookup(struct skiplist *list, char *data);
int skiplist_notfull(struct skiplist *list);
void skiplist_dump(struct skiplist *list);
void skiplist_free(struct skiplist *list);


#endif
