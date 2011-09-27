/* Level lru cache: inlcude new-level and old-level two levels
 * If one's hits >MAX,it will be moved to new-level
 * It's a real non-dirty shit,and likely atomic energy level transition
 */
#include <stdio.h>
#include <stdlib.h>
#include "llru.h"

#define MAXHITS (3)
#define PRIME	(16785407)
#define RATIO	(0.618)

static struct ht *_ht;
static struct level *_level_old;
static struct level *_level_new;

void llru_init(size_t buffer_size)
{
	ht_init(_ht,PRIME);
	
	size_t size_n=buffer_size*RATIO;
	size_t size_o=buffer_size-size_n;

	_level_old->count=0;
	_level_old->size=size_o;
	_level_old->first=NULL;
	_level_old->last=NULL;
	_level_old->ht=_ht;
	
	_level_new->count=0;
	_level_new->size=size_n;
	_level_new->first=NULL;
	_level_new->last=NULL;
	_level_new->ht=_ht;
}

static void llru_set_node(struct level_node *n)
{
	if(n!=NULL){
		if(n->hits==-1){
			level_remove_link(_level_new,n);
			level_set_head(_level_new,n);
		}else{

			n->hits++;
			level_remove_link(_level_old,n);
			if(n->hits>MAXHITS){
				level_set_head(_level_new,n);
				n->hits=-1;
			}else
				level_set_head(_level_old,n);
		}
	}
}


void llru_set(const char *k,void *v)
{
	struct level_node *n=ht_get(_ht,k);
	if(n!=NULL)
		llru_set_node(n);
}


void* llru_get(const char *k)
{
	struct level_node *n=ht_get(_ht,k);
	if(n!=NULL)
		llru_set_node(n);
	return n->value;
}

void llru_remove(const char* k)
{
	struct level_node *n=ht_get(_ht,k);
	if(n!=NULL){

		ht_remove(_ht,k);
		if(n->hits==-1)
			level_free_node(_level_new,n);
		else
			level_free_node(_level_old,n);
	}
}
