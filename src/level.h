#ifndef _LEVEL_H
#define _LEVEL_H
#include "ht.h"

struct level{
	size_t size;
	size_t count;

	struct level_node *first;
	struct level_node *last;

	struct ht *ht;
};

struct level_node{
	char*	key;
	void*	value;
	int	hits;

	struct level_node *nxt;
	struct level_node *pre;
};


void 		level_set_head(struct level *level,struct level_node *node);
void		level_remove_link(struct level *,struct level_node *node);
void		level_free_last(struct level *level);
void 		level_free_node(struct level *level,struct level_node *node);
void 		level_free(struct level *level);

#endif
