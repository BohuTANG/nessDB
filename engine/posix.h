/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_POSIX_H_
#define nessDB_POSIX_H_

#include "internal.h"
#include "atomic.h"
#include "debug.h"

#define pthread_call(result, msg)					\
	do {								\
		if (result != 0) {					\
			__ERROR("[%s], %s", strerror(result), msg);	\
			abort();					\
		}							\
	} while(0)

/*******************************
 * mutex
 ******************************/
typedef struct {
	pthread_mutex_t mtx;
} ness_mutex_t;


typedef struct {
	ness_mutex_t aligned_mtx  __attribute__((__aligned__(64)));
} ness_mutex_aligned_t;


static inline void mutex_init(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_init(&mutex->mtx, NULL), "mutex init");
}

static inline void mutex_lock(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_lock(&mutex->mtx), "mutex lock");
}

static inline void mutex_unlock(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_unlock(&mutex->mtx), "mutex unlock");
}

static inline void lock_destroy(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_destroy(&mutex->mtx), "mutex destroy");
}


/*******************************
 * cond
 ******************************/
typedef struct {
	pthread_cond_t cond;
	pthread_mutex_t mtx;
	pthread_condattr_t  attr;
} ness_cond_t;

static inline void cond_init(ness_cond_t *cond) {
    pthread_mutex_init(&cond->mtx, NULL);
    pthread_call(pthread_cond_init(&cond->cond, &cond->attr), "cond init");
}

static inline void cond_wait(ness_cond_t *cond) {
	pthread_call(pthread_cond_wait(&cond->cond, &cond->mtx), "cond wait");
}

static inline void cond_signal(ness_cond_t *cond) {
	pthread_call(pthread_cond_signal(&cond->cond), "cond signal");
}

static inline void cond_signalall(ness_cond_t *cond) {
	pthread_call(pthread_cond_broadcast(&cond->cond), "cond broadcast");
}

static inline void cond_destroy(ness_cond_t *cond) {
	pthread_call(pthread_cond_destroy(&cond->cond), "cond destroy");
}

/*******************************
 * lock
 ******************************/

typedef struct {
	pthread_rwlock_t lock;
	int num_readers;
	int num_writers;
	int num_want_read;
	int num_want_write;
} ness_rwlock_t;

static inline void rwlock_init(ness_rwlock_t *rwlock)
{
	rwlock->num_readers = 0;
	rwlock->num_writers = 0;
	rwlock->num_want_read = 0;
	rwlock->num_want_write = 0;
	pthread_call(pthread_rwlock_init(&rwlock->lock, NULL), "ness rwlock init");
}

static inline void read_lock(ness_rwlock_t *rwlock)
{
	atomic32_increment(&rwlock->num_want_read);
	pthread_call(pthread_rwlock_rdlock(&rwlock->lock), "ness read lock");
	atomic32_decrement(&rwlock->num_want_read);
	atomic32_increment(&rwlock->num_readers);
}

static inline int try_read_lock(ness_rwlock_t *rwlock)
{
	if (pthread_rwlock_tryrdlock(&rwlock->lock) == 0) {
		atomic32_increment(&rwlock->num_readers);
		return 1;
	} else {
		return 0;
	}
}

static inline void read_unlock(ness_rwlock_t *rwlock)
{
	pthread_call(pthread_rwlock_unlock(&rwlock->lock), "ness read unlock");
	atomic32_decrement(&rwlock->num_readers);
}

static inline void write_lock(ness_rwlock_t *rwlock)
{
	atomic32_increment(&rwlock->num_want_write);
	pthread_call(pthread_rwlock_wrlock(&rwlock->lock), "ness write lock");
	atomic32_decrement(&rwlock->num_want_write);
	atomic32_increment(&rwlock->num_writers);
}

static inline int try_write_lock(ness_rwlock_t *rwlock)
{
	if (pthread_rwlock_trywrlock(&rwlock->lock) == 0) {
		atomic32_increment(&rwlock->num_writers);
		return 1;
	} else {
		return 0;
	}
}

static inline void write_unlock(ness_rwlock_t *rwlock)
{
	pthread_call(pthread_rwlock_unlock(&rwlock->lock), "ness write unlock");
	atomic32_decrement(&rwlock->num_writers);
}

static inline void rwlock_destroy(ness_rwlock_t *rwlock)
{
	pthread_call(pthread_rwlock_destroy(&rwlock->lock), "ness rwlock destroy");
}


/*******************************
 * cron
 ******************************/
typedef void* (*func)(void* arg);
struct cron {
	func f;
	void *arg;
	uint32_t isalive;
	uint32_t period_ms;
	pthread_t thread;
	pthread_mutex_t mtx;
	pthread_cond_t cond;
	struct timespec last_call;
};

struct cron *cron_new(func f, uint32_t period_ms);
int cron_start(struct cron *cron, void *arg);
int cron_change_period(struct cron *cron, uint32_t period_ms);
int cron_stop(struct cron *cron);
void cron_signal(struct cron *cron);
void cron_free(struct cron *cron);


/*******************************
 * time
 ******************************/
void gettime (struct timespec *a);
long long time_diff_ms(struct timespec start, struct timespec end);

#endif /* nessDB_POSIX_H_ */
