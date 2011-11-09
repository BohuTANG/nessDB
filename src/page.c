/*
 * Page is a page-cache for random-writing.
 * Drafted by BohuTANG
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "level.h"
#include "page.h"

#define PRIME (16785407)

void page_init(struct page *page,int buffer_size)
{
	struct ht *h=calloc(1,sizeof(struct ht));
	struct level *list=calloc(1,sizeof(struct level));
	
	ht_init(h,PRIME,INT);

	list->count=0;
	list->allow_size=buffer_size;
	list->used_size=0;
	list->first=NULL;
	list->last=NULL;
	
	page->count=0;
	page->ht=h;
	page->list=list;
}


int page_set(struct page *page,int offset ,struct btree_table *table)
{
	if(table!=NULL){
		if(page->list->used_size>page->list->allow_size){
			return -1;
		}
		
	}

	return 1;
}



