/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"

/* time functions */
void ngettime(struct timespec *a)
{
	struct timeval tv;

	gettimeofday(&tv, 0);
	a->tv_sec  = tv.tv_sec;
	a->tv_nsec = tv.tv_usec * 1000LL;
}

struct timespec diff_timespec(struct timespec start, struct timespec end)
{
	struct timespec result;

	if (end.tv_nsec < start.tv_nsec) {
		result.tv_nsec = 1000000000 + end.tv_nsec -
		                 start.tv_nsec;
		result.tv_sec = end.tv_sec - 1 -
		                start.tv_sec;
	} else {
		result.tv_nsec = end.tv_nsec -
		                 start.tv_nsec;
		result.tv_sec = end.tv_sec -
		                start.tv_sec;
	}

	return result;
}

static inline long long millisec_elapsed(struct timespec diff)
{
	return ((long long)diff.tv_sec * 1000) + (diff.tv_nsec / 1000000);
}

long long time_diff_ms(struct timespec start, struct timespec end)
{
	return millisec_elapsed(diff_timespec(start, end));
}

/* cron functions */
struct cron *cron_new(func f, uint32_t period_ms)
{
	struct cron *cron;

	cron = xcalloc(1, sizeof(*cron));
	cron->f = f;
	cron->isalive = 0U;
	cron->period_ms = period_ms;

	/* init */
	pthread_mutex_init(&cron->mtx, NULL);
	pthread_cond_init(&cron->cond, NULL);
	ngettime(&cron->last_call);

	return cron;
}

static void *_do_cron(void *arg)
{
	struct cron *cron = (struct cron*)arg;

	pthread_mutex_lock(&cron->mtx);
	while (1) {
		if (!cron->isalive) {
			pthread_mutex_unlock(&cron->mtx);
			return NULL;
		}

		if (cron->period_ms == 0) {
			pthread_cond_wait(&cron->cond, &cron->mtx);
		} else {

			struct timespec to_wakeup;
			uint32_t p_ms = cron->period_ms;

			to_wakeup = cron->last_call;
			to_wakeup.tv_nsec += (p_ms * 1000000);
			to_wakeup.tv_sec += (to_wakeup.tv_nsec / 1000000000);
			to_wakeup.tv_nsec = (to_wakeup.tv_nsec % 1000000000);

			int r = pthread_cond_timedwait(&cron->cond, &cron->mtx, &to_wakeup);
			if (r == 0 || r == ETIMEDOUT) {
				if (cron->isalive) {
					pthread_mutex_unlock(&cron->mtx);
					cron->f(cron->arg);
					pthread_mutex_lock(&cron->mtx);
					ngettime(&cron->last_call);
				}
			}
		}
	}
	pthread_mutex_unlock(&cron->mtx);

	return NULL;
}

int cron_start(struct cron *cron, void *arg)
{
	pthread_attr_t attr;

	if (cron->isalive) return -1;

	cron->isalive = 1U;
	cron->arg = arg;
	pthread_attr_init(&attr);
	if (pthread_create(&cron->thread, &attr, _do_cron, cron) != 0) {
		fprintf(stderr, "%s", "can't initialize cron.");
		return -1;
	}

	return 1;
}

int cron_change_period(struct cron *cron, uint32_t period_ms)
{
	pthread_mutex_lock(&cron->mtx);
	cron->period_ms = period_ms;
	pthread_cond_signal(&cron->cond);
	pthread_mutex_unlock(&cron->mtx);

	return 1;
}

int cron_stop(struct cron *cron)
{
	pthread_mutex_lock(&cron->mtx);
	cron->isalive = 0;
	pthread_cond_signal(&cron->cond);
	pthread_mutex_unlock(&cron->mtx);

	return 1;
}

void cron_signal(struct cron *cron)
{
	pthread_mutex_lock(&cron->mtx);
	pthread_cond_signal(&cron->cond);
	pthread_mutex_unlock(&cron->mtx);
}

void cron_free(struct cron *cron)
{
	if (cron->isalive)
		cron_stop(cron);

	pthread_join(cron->thread, NULL);

	/* TODO (BohuTANG): here is helgrind warning
	pthread_cond_destroy(&cron->cond);
	*/
	pthread_mutex_destroy(&cron->mtx);
	xfree(cron);
}


