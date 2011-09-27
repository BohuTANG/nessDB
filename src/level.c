#include <stdio.h>
#include <stdlib.h>
#include "level.h"

void level_set_head(struct level *level,struct level_node *n)
{
	level->count++;
	if(level->first==NULL){
		level->first=n;
	}else{
		n->pre=NULL;
		n->nxt=level->first;
		level->first=n;
	}
}

void level_remove_link(struct level *level,struct level_node *n)
{
	level->count--;
	if(n->pre==NULL){
		level->first=n->nxt;
		n->nxt=NULL;
	}else{
		if(n->nxt==NULL){
			level->last=n->pre;
			n->pre=NULL;
		}else{
			n->pre->nxt=n->nxt;
			n->nxt=NULL;
			n->pre=NULL;
		}
	}

}	


void level_free_node(struct level *level,struct level_node *n)
{
	if(n){
		level_remove_link(level,n);
		if(n->key)
			free(n->key);
		if(n->value)
			free(n->value);
		free(n);
	}
}

void level_free_last(struct level *level)
{
	struct level_node *n=level->last;
        level_free_node(level,n);
}
