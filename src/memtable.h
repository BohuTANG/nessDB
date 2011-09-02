#ifndef _MEMTABLE_H
#define _MENTABLE_H

typedef enum {USING=0,DELING,LOCKING} TABLE_INGS;
typedef enum {ADD=0,DELETE} OP_TYPE;

struct memtable
{
	size_t 		count;
	size_t		size;
	size_t		cap;
	TABLE_INGS	ings;
};

void		init(struct memtable *table);
void		set(struct memtable *table,OP_TYPE type);
uint64_t	get(struct memtable *table,const char *k);

#endif
