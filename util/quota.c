/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

struct quota *quota_new(uint64_t limit)
{
	struct quota *q = xcalloc(1, sizeof(*q));

	mutex_init(&q->lock);
	cond_init(&q->cond);
	q->limit = limit;
	q->hw_value = (4 * limit) / 5;

	return q;
}

int quota_add(struct quota *q, uint64_t v)
{
	mutex_lock(&q->lock);
	q->used += v;
	mutex_unlock(&q->lock);

	return NESS_OK;
}

int quota_remove(struct quota *q, uint64_t v)
{
	mutex_lock(&q->lock);
	q->used -= v;
	mutex_unlock(&q->lock);
	cond_signal(&q->cond);

	return NESS_OK;
}

int quota_wait(struct quota *q)
{
	cond_wait(&q->cond, &q->lock);

	return NESS_OK;
}

int quota_signal(struct quota *q)
{
	cond_signalall(&q->cond);

	return NESS_OK;
}


QUOTA_STATE quota_state(struct quota *q)
{
	QUOTA_STATE state = QUOTA_STATE_NONE;

	mutex_lock(&q->lock);
	if (q->used >= q->hw_value)
		state = QUOTA_STATE_NEED_EVICT;

	if (q->used >= q->limit)
		state = QUOTA_STATE_MUST_EVICT;
	mutex_unlock(&q->lock);

	return state;
}

int quota_change_limit(struct quota *q, uint64_t limit)
{
	mutex_lock(&q->lock);
	q->hw_value = (4 * limit) / 5;
	q->limit = limit;
	mutex_unlock(&q->lock);

	return NESS_OK;
}

void quota_free(struct quota *q)
{
	cond_destroy(&q->cond);
	mutex_destroy(&q->lock);
	xfree(q);
}
