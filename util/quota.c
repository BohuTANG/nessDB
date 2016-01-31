/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

struct quota *quota_new(uint64_t limit)
{
	struct quota *q = xcalloc(1, sizeof(*q));

	ness_mutex_init(&q->lock);
	ness_cond_init(&q->cond);
	q->limit = limit;
	q->hw_value = (4 * limit) / 5;

	return q;
}

int quota_add(struct quota *q, uint64_t v)
{
	atomic_fetch_and_add(&q->used, v);
	return NESS_OK;
}

int quota_remove(struct quota *q, uint64_t v)
{
	atomic_fetch_and_sub(&q->used, v);
	ness_cond_signal(&q->cond);

	return NESS_OK;
}

int quota_wait(struct quota *q)
{
	ness_mutex_lock(&q->lock);
	ness_cond_wait(&q->cond, &q->lock);
	ness_mutex_unlock(&q->lock);

	return NESS_OK;
}

int quota_signal(struct quota *q)
{
	ness_cond_signalall(&q->cond);

	return NESS_OK;
}


QUOTA_STATE quota_state(struct quota *q)
{
	uint64_t used = atomic_fetch_and_add(&q->used, 0);
	QUOTA_STATE state = QUOTA_STATE_NONE;

	if (used >= q->hw_value)
		state = QUOTA_STATE_NEED_EVICT;

	if (used >= q->limit)
		state = QUOTA_STATE_MUST_EVICT;

	return state;
}

int quota_change_limit(struct quota *q, uint64_t limit)
{
	ness_mutex_lock(&q->lock);
	q->hw_value = (4 * limit) / 5;
	q->limit = limit;
	ness_mutex_unlock(&q->lock);

	return NESS_OK;
}

void quota_free(struct quota *q)
{
	ness_cond_destroy(&q->cond);
	ness_mutex_destroy(&q->lock);
	xfree(q);
}
