#ifndef _SKIPLIST_H
#define _SKIPLIST_H

#include "util.h"

#define MAXLEVEL (15)
#define SKIP_KSIZE (256)

typedef enum {ADD,DEL} OPT;

struct skipnode{
    char key[SKIP_KSIZE];
	uint64_t val;
	OPT opt;                   
    struct skipnode *forward[1]; 
	struct skipnode *next;
};

struct skiplist{
	struct  skipnode *hdr;                 
	int count;
	int size;
	int level; 
	char pool_embedded[1024];
	struct pool *pool;
};

struct skiplist *skiplist_new(size_t size);
void skiplist_init(struct skiplist *list);
int skiplist_insert(struct skiplist *list, struct slice *sk, uint64_t val, OPT opt);
int skiplist_notfull(struct skiplist *list);
void skiplist_dump(struct skiplist *list);
void skiplist_free(struct skiplist *list);


#endif
