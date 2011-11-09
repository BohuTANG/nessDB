#ifndef _LEVEL_H
#define _LEVEL_H
#include "ht.h"

struct level{
	size_t count;
	size_t allow_size;
	size_t used_size;

	struct level_node *first;
	struct level_node *last;

};

struct level_node{
	void*	key;
	void*	value;
	int	hits;
	int	size;

	struct level_node *nxt;
	struct level_node *pre;
};


void 		level_set_head(struct level *level,struct level_node *node);
void		level_remove_link(struct level *,struct level_node *node);
void		level_free_last(struct level *level);
void 		level_free_node(struct level *level,struct level_node *node);
void 		level_free(struct level *level);

#endif
