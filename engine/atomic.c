/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include <pthread.h>
#include "atomic.h"

static pthread_mutex_t atomic_mtx __attribute__((__unused__)) = PTHREAD_MUTEX_INITIALIZER;
int atomic32_increment(int *dest)
{
	int r;

#ifdef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4
	r = __sync_add_and_fetch(dest, 1U);
#else
	pthread_mutex_lock(&atomic_mtx);
	*dest += 1;
	r = *dest;
	pthread_mutex_unlock(&atomic_mtx);
#endif
	return r;
}

int atomic32_decrement(int *dest)
{
	int r;

#ifdef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4
	r = __sync_sub_and_fetch(dest, 1U);
#else
	pthread_mutex_lock(&atomic_mtx);
	*dest -= 1;
	r = *dest;
	pthread_mutex_unlock(&atomic_mtx);
#endif
	return r;

}

uint64_t atomic64_increment(uint64_t *dest)
{
	uint64_t r;

#ifdef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8
	r = __sync_add_and_fetch(dest, 1U);
#else
	pthread_mutex_lock(&atomic_mtx);
	*dest += 1;
	r = *dest;
	pthread_mutex_unlock(&atomic_mtx);
#endif
	return r;
}

uint64_t atomic64_decrement(uint64_t *dest)
{
	uint64_t r;

#ifdef __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8
	r = __sync_sub_and_fetch(dest, 1U);
#else
	pthread_mutex_lock(&atomic_mtx);
	*dest -= 1;
	r = *dest;
	pthread_mutex_unlock(&atomic_mtx);
#endif
	return r;
}
