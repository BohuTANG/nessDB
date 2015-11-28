/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

struct quota *quota_new(uint64_t limit, double highwater_percent)
{
	struct quota *q = xcalloc(1, sizeof(*q));

	mutex_init(&q->lock);
	cond_init(&q->cond);
	q->limit = limit;
	q->hw_percent = highwater_percent;
	q->hw_value = limit * highwater_percent;

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
	cond_signal(&q->cond);
	mutex_unlock(&q->lock);

	return NESS_OK;
}

int quota_check_maybe_wait(struct quota *q)
{
	mutex_lock(&q->lock);
	if (q->used >= q->limit)
		cond_wait(&q->cond, &q->lock);
	mutex_unlock(&q->lock);

	return NESS_OK;
}

int quota_fullwater(struct quota *q)
{
	int ret = 0;

	mutex_lock(&q->lock);
	ret = q->used >= q->limit;
	mutex_unlock(&q->lock);

	return ret;
}
int quota_highwater(struct quota *q)
{
	int ret = 0;

	mutex_lock(&q->lock);
	ret = q->used >= q->hw_value;
	mutex_unlock(&q->lock);

	return ret;
}

int quota_change_limit(struct quota *q, uint64_t limit)
{
	mutex_lock(&q->lock);
	q->hw_value = limit * q->hw_percent;
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
