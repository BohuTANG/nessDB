/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
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
#define atomic_fetch_and_inc(t) __sync_fetch_and_add (t, 1)
#define atomic_fetch_and_dec(t) __sync_fetch_and_sub (t, 1)
#define atomic_fetch_and_add(t, v) __sync_fetch_and_add (t, v)
#define atomic_fetch_and_sub(t, v) __sync_fetch_and_sub (t, v)
#define atomic_compare_and_swap(t, old, new) __sync_bool_compare_and_swap (t, old, new)
#define atomic_release(t) __sync_lock_release(t)

/*******************************
 * mutex
 ******************************/
typedef struct ness_mutex {
	pthread_mutex_t mtx;
} ness_mutex_t;


typedef struct {
	ness_mutex_t aligned_mtx;
} ness_mutex_aligned_t NESSPACKED;


static inline void ness_mutex_init(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_init(&mutex->mtx, NULL), "mutex init");
}

static inline void ness_mutex_lock(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_lock(&mutex->mtx), "mutex lock");
}

static inline void ness_mutex_unlock(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_unlock(&mutex->mtx), "mutex unlock");
}

static inline void ness_mutex_destroy(ness_mutex_t *mutex)
{
	pthread_call(pthread_mutex_destroy(&mutex->mtx), "mutex destroy");
}

/*******************************
 * spin
 ******************************/

typedef uint32_t ness_spinlock;

#if defined(__x86_64__) || defined(__i386) || defined(_X86_)
# define CPU_PAUSE __asm__ ("pause")
#else
# define CPU_PAUSE do { } while(0)
#endif

static inline void ness_spinlock_init(ness_spinlock *l)
{
	*l = 0;
}

static inline void ness_spinlock_free(ness_spinlock *l)
{
	*l = 0;
}

static inline void ness_spinwlock(ness_spinlock *l)
{
	if (!atomic_compare_and_swap(l, 0, 1)) {
		for (;;) {
			CPU_PAUSE;
			if (atomic_compare_and_swap(l, 0, 1))
				break;
			usleep(150);
		}
	}
}

static inline void ness_spinwunlock(ness_spinlock *l)
{
	atomic_release(l);
}

static inline void ness_spinrlock(ness_spinlock *l)
{
	atomic_fetch_and_inc(l);
}

static inline void ness_spinrunlock(ness_spinlock *l)
{
	atomic_fetch_and_dec(l);
}

static inline void ness_spinlock_waitfree(ness_spinlock *l)
{
	if (!atomic_compare_and_swap(l, 0, 0)) {
		for (;;) {
			CPU_PAUSE;
			if (atomic_compare_and_swap(l, 0, 0))
				break;
			usleep(100);
		}
	}
}

/*******************************
 * cond
 ******************************/
typedef struct {
	pthread_cond_t cond;
	pthread_condattr_t  attr;
} ness_cond_t;

static inline void ness_cond_init(ness_cond_t *cond)
{
	pthread_call(pthread_cond_init(&cond->cond, &cond->attr), "cond init");
}

static inline void ness_cond_wait(ness_cond_t *cond, ness_mutex_t *mtx)
{
	pthread_call(pthread_cond_wait(&cond->cond, &mtx->mtx), "cond wait");
}

static inline void ness_cond_signal(ness_cond_t *cond)
{
	pthread_call(pthread_cond_signal(&cond->cond), "cond signal");
}

static inline void ness_cond_signalall(ness_cond_t *cond)
{
	pthread_call(pthread_cond_broadcast(&cond->cond), "cond broadcast");
}

static inline void ness_cond_destroy(ness_cond_t *cond)
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
	ness_mutex_t *mtx;
} ness_rwlock_t;

static inline void ness_rwlock_init(struct rwlock *rwlock, ness_mutex_t *mtx)
{
	rwlock->reader = rwlock->want_reader = 0;
	rwlock->writer = rwlock->want_writer = 0;
	ness_cond_init(&rwlock->wait_read);
	ness_cond_init(&rwlock->wait_write);
	rwlock->mtx = mtx;
}

static inline void ness_rwlock_destroy(struct rwlock *rwlock)
{
	assert(rwlock->reader == 0 && rwlock->want_reader == 0);
	assert(rwlock->writer == 0 && rwlock->want_writer == 0);
	ness_cond_destroy(&rwlock->wait_read);
	ness_cond_destroy(&rwlock->wait_write);
}

static inline void ness_rwlock_read_lock(struct rwlock *rwlock)
{
	ness_mutex_lock(rwlock->mtx);
	if (rwlock->writer || rwlock->want_writer) {
		rwlock->want_reader++;
		while (rwlock->writer || rwlock->want_writer) {
			ness_cond_wait(&rwlock->wait_read, rwlock->mtx);
		}
		rwlock->want_reader--;
	}
	rwlock->reader++;
	ness_mutex_unlock(rwlock->mtx);
}

static inline int ness_rwlock_try_read_lock(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->writer
		+ rwlock->want_writer;
	if(users == 0)
		rwlock->reader++;
	ness_mutex_unlock(rwlock->mtx);

	return (users == 0);
}

static inline void ness_rwlock_read_unlock(struct rwlock *rwlock)
{
	ness_mutex_lock(rwlock->mtx);
	assert(rwlock->reader > 0);
	assert(rwlock->writer == 0);
	rwlock->reader--;
	if (rwlock->want_writer > 0)
		ness_cond_signal(&rwlock->wait_write);
	ness_mutex_unlock(rwlock->mtx);
}

static inline void ness_rwlock_write_lock(struct rwlock *rwlock)
{
	ness_mutex_lock(rwlock->mtx);
	if (rwlock->reader || rwlock->writer) {
		rwlock->want_writer++;
		while (rwlock->reader || rwlock->writer)
			ness_cond_wait(&rwlock->wait_write, rwlock->mtx);
		rwlock->want_writer--;
	}
	rwlock->writer++;
	ness_mutex_unlock(rwlock->mtx);
}

static inline int ness_rwlock_try_write_lock(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->reader
		+ rwlock->want_reader
		+ rwlock->writer
		+ rwlock->want_writer;
	if(users == 0)
		rwlock->writer++;
	ness_mutex_unlock(rwlock->mtx);

	return (users == 0);
}

static inline void ness_rwlock_write_unlock(struct rwlock *rwlock)
{
	ness_mutex_lock(rwlock->mtx);
	assert(rwlock->reader == 0);
	assert(rwlock->writer == 1);
	rwlock->writer--;
	if (rwlock->want_writer)
		ness_cond_signal(&rwlock->wait_write);
	else
		ness_cond_signalall(&rwlock->wait_read);
	ness_mutex_unlock(rwlock->mtx);
}

static inline int ness_rwlock_users(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->reader
		+ rwlock->want_reader
		+ rwlock->writer
		+ rwlock->want_writer;
	ness_mutex_unlock(rwlock->mtx);

	return users;
}

static inline int ness_rwlock_readers(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->reader;
	ness_mutex_unlock(rwlock->mtx);

	return users;
}

static inline int ness_rwlock_blocked_readers(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->want_reader;
	ness_mutex_unlock(rwlock->mtx);

	return users;
}

static inline int ness_rwlock_writers(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->writer;
	ness_mutex_unlock(rwlock->mtx);

	return users;
}

static inline int ness_rwlock_blocked_writers(struct rwlock *rwlock)
{
	int users = 0;

	ness_mutex_lock(rwlock->mtx);
	users = rwlock->want_writer;
	ness_mutex_unlock(rwlock->mtx);

	return users;
}

/*******************************
 * cron
 ******************************/
typedef void* (*func)(void* arg);
struct ness_cron {
	func f;
	void *arg;
	uint32_t isalive;
	uint32_t period_ms;
	pthread_t thread;
	pthread_mutex_t mtx;
	pthread_cond_t cond;
	struct timespec last_call;
};

struct ness_cron *ness_cron_new(func f, uint32_t period_ms);
int ness_cron_start(struct ness_cron *cron, void *arg);
int ness_cron_change_period(struct ness_cron *cron, uint32_t period_ms);
int ness_cron_stop(struct ness_cron *cron);
void ness_cron_signal(struct ness_cron *cron);
void ness_cron_free(struct ness_cron *cron);

/*******************************
 * time
 ******************************/
void ness_gettime(struct timespec *a);
long long ness_time_diff_ms(struct timespec start, struct timespec end);

#endif /* nessDB_POSIX_H_ */
