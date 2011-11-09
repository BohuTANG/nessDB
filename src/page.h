#ifndef _PAGE_H
#define _PAGE_H

#include "storage.h"
#include "level.h"
#include "ht.h"

struct page_item{
	int offset;
	int w_count;
	int level;

	struct btree_table *table;
};

struct page{
	int count;
	struct ht *ht;
	struct level *list;
};

void page_init(struct page *page,int buffsize);
int page_set(struct page *page,int offset,struct btree_table *tbl);
struct btree_table *page_get(struct page,int offset);
void page_remove(struct page *page,int offset);
void page_free(struct page *page);

#endif
