/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "atomic.h"

#ifdef OS_MACOSX
#include <libkern/OSAtomic.h>
#endif

/**
 * memory-barrier effect (compiler memory barrier)
 *
 * @supports: Linux and MacOs
 * just provides an atomic read-modify-write sequence
 *
 * barrier.c is a platform-dependent implementation
 * on x86/64 platform, it guarantees:
 * LoadStore, LoadLoad, and StoreStore
 * http://preshing.com/20120930/weak-vs-strong-memory-models/
 *
 * TODO(BohuTANG): for more platforms
 */
void memory_barrier()
{

	/* linux */
#ifdef OS_LINUX
	/* just compiler barrier */
	__asm__ __volatile__("" : : : "memory");

	/* mac os */
#elif OS_MACOSX
	OSMemoryBarrier();

	/* android */
#elif OS_ANDROID
	(*(LinuxKernelMemoryBarrierFunc)0xffff0fa0)();

#else
#error please implement barrier for this platform.
#endif
}

void *acquire_load(void *rep)
{
	void *result = rep;

	memory_barrier();
	return result;
}

void release_store(void **from, void *to)
{
	memory_barrier();
	*from = to;
}

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
