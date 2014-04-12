/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_MB_H_
#define nessDB_MB_H_

/* memory barrier */
void memory_barrier();
void *acquire_load(void *rep);
void release_store(void **from, void *to);

#endif /* nessDB_MB_H_ */
