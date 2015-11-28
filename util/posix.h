/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_POSIX_H_
#define nessDB_POSIX_H_

#define pthread_call(result, msg)					\
	do {								\
		if (result != 0) {					\
			abort();					\
		}							\
	} while(0)

/*******************************
 * atomic
 ******************************/
int atomic32_increment(int *dest);
int atomic32_decrement(int *dest);
uint64_t atomic64_increment(uint64_t *dest);
uint64_t atomic64_decrement(uint64_t *dest);
uint64_t atomic64_add(uint64_t *dest, uint64_t add);
uint64_t atomic64_sub(uint64_t *dest, uint64_t sub);

/*******************************
 * mutex
 ******************************/
typedef struct ness_mutex {
	pthread_mutex_t mtx;
} ness_mutex_t;


typedef struct {
	ness_mutex_t aligned_mtx;
} ness_mutex_aligned_t NESSPACKED;


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

static inline void mutex_destroy(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_destroy(&mutex->mtx), "mutex destroy");
}

/*******************************
 * cond
 ******************************/
typedef struct {
	pthread_cond_t cond;
	pthread_condattr_t  attr;
} ness_cond_t;

static inline void cond_init(ness_cond_t *cond)
{
	pthread_call(pthread_cond_init(&cond->cond, &cond->attr), "cond init");
}

static inline void cond_wait(ness_cond_t *cond, ness_mutex_t *mtx)
{
	pthread_call(pthread_cond_wait(&cond->cond, &mtx->mtx), "cond wait");
}

static inline void cond_signal(ness_cond_t *cond)
{
	pthread_call(pthread_cond_signal(&cond->cond), "cond signal");
}

static inline void cond_signalall(ness_cond_t *cond)
{
	pthread_call(pthread_cond_broadcast(&cond->cond), "cond broadcast");
}

static inline void cond_destroy(ness_cond_t *cond)
{
	pthread_call(pthread_cond_destroy(&cond->cond), "cond destroy");
}

/*******************************
 * rwlock
 ******************************/

typedef struct rwlock {
	int reader;
	int want_reader;
	ness_cond_t wait_read;
	int writer;
	int want_writer;
	ness_cond_t wait_write;
} ness_rwlock_t;

static inline void ness_rwlock_init(struct rwlock *rwlock)
{
	rwlock->reader = rwlock->want_reader = 0;
	rwlock->writer = rwlock->want_writer = 0;
	cond_init(&rwlock->wait_read);
	cond_init(&rwlock->wait_write);
}

static inline void ness_rwlock_destroy(struct rwlock *rwlock)
{
	assert(rwlock->reader == 0 && rwlock->want_reader == 0);
	assert(rwlock->writer == 0 && rwlock->want_writer == 0);
	cond_destroy(&rwlock->wait_read);
	cond_destroy(&rwlock->wait_write);
}

static inline void rwlock_read_lock(struct rwlock *rwlock, ness_mutex_t *mutex)
{
	if (rwlock->writer || rwlock->want_writer) {
		rwlock->want_reader++;
		while (rwlock->writer || rwlock->want_writer) {
			cond_wait(&rwlock->wait_read, mutex);
		}
		rwlock->want_reader--;
	}
	rwlock->reader++;
}

static inline void rwlock_read_unlock(struct rwlock *rwlock)
{
	assert(rwlock->reader > 0);
	assert(rwlock->writer == 0);
	rwlock->reader--;
	if (rwlock->want_writer > 0)
		cond_signal(&rwlock->wait_write);
}

static inline void rwlock_write_lock(struct rwlock *rwlock, ness_mutex_t *mutex)
{
	if (rwlock->reader || rwlock->writer) {
		rwlock->want_writer++;
		while (rwlock->reader || rwlock->writer)
			cond_wait(&rwlock->wait_write, mutex);
		rwlock->want_writer--;
	}
	rwlock->writer++;
}

static inline void rwlock_write_unlock(struct rwlock *rwlock)
{
	assert(rwlock->reader == 0);
	assert(rwlock->writer == 1);
	rwlock->writer--;
	if (rwlock->want_writer)
		cond_signal(&rwlock->wait_write);
	else
		cond_signalall(&rwlock->wait_read);
}

static inline int rwlock_users(struct rwlock *rwlock)
{
	return rwlock->reader + rwlock->want_reader + rwlock->writer + rwlock->want_writer;
}

static inline int rwlock_readers(struct rwlock *rwlock)
{
	return rwlock->reader;
}

static inline int rwlock_blocked_readers(struct rwlock *rwlock)
{
	return rwlock->want_reader;
}

static inline int rwlock_writers(struct rwlock *rwlock)
{
	return rwlock->writer;
}

static inline int rwlock_blocked_writers(struct rwlock *rwlock)
{
	return rwlock->want_writer;
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
void ngettime(struct timespec *a);
long long time_diff_ms(struct timespec start, struct timespec end);

#endif /* nessDB_POSIX_H_ */
