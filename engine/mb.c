/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "mb.h"

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

/* compiler barrier */
void *acquire_load(void *rep)
{
	void *result = rep;

	memory_barrier();
	return result;
}

/* compiler barrier */
void release_store(void **from, void *to)
{
	memory_barrier();
	*from = to;
}
