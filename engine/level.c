/*
 * nessDB storage engine
 * Copyright (c) 2011, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with BSD. See COPYING.BSD file.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include "level.h"

struct level *level_creat()
{
	struct level *lev;
	lev = malloc(sizeof(struct level));
	return lev;
}

void level_set_head(struct level *level, struct level_node *n)
{
	level->count++;
	if (level->first == NULL) {
		level->first = n;
	} else {
		n->pre = NULL;
		n->nxt = level->first;
		level->first = n;
	}
}

void level_remove_link(struct level *level, struct level_node *n)
{
	level->count--;
	if (n->pre == NULL) {
		level->first = n->nxt;
		n->nxt = NULL;
	} else {
		if (n->nxt == NULL) {
			level->last = n->pre;
			n->pre = NULL;
		} else {
			n->pre->nxt = n->nxt;
			n->nxt = NULL;
			n->pre = NULL;
		}
	}

}	


void level_free_node(struct level *level, struct level_node *n)
{
	if (n) {
		level->used_size -= n->size;
		level_remove_link(level, n);

		if (n->sk.data)
			free(n->sk.data);
		if (n->sv.data)
			free(n->sv.data);
		free(n);
	}
}

void level_free_last(struct level *level)
{
	struct level_node *n;
	n = level->last;
	level_free_node(level, n);
}
