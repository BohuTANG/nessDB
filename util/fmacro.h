/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_FMACRO_H_
#define nessDB_FMACRO_H_

#if (__i386 || __amd64 || __powerpc__) && __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if defined(__clang__)
#define HAVE_ATOMIC
#endif
#if (defined(__GLIBC__) && defined(__GLIBC_PREREQ))
#if (GNUC_VERSION >= 40100 && __GLIBC_PREREQ(2, 6))
#define HAVE_ATOMIC
#endif
#endif
#endif

#if defined (__linux__)
#if !defined(O_DIRECT)
#define O_DIRECT O_SYNC
#endif
#define HAVE_DIRECTIO
#endif

#if defined(__linux__)
#define _GNU_SOURCE
#endif
#if defined(__linux__) || defined(__OpenBSD__)
#define _XOPEN_SOURCE 700
#endif

#define nesslikely(EXPR) __builtin_expect(!! (EXPR), 1)
#define nessunlikely(EXPR) __builtin_expect(!! (EXPR), 0)
#define NESSPACKED  __attribute__((__aligned__(64)))

#define FILE_NAME_MAXLEN (256)

#endif /* nessDB_FMACRO_H_ */
