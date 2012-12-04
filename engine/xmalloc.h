#ifndef __nessDB_XMALLOC_H
#define __nessDB_XMALLOC_H

void *xmalloc(size_t n);
void *xcalloc(size_t n, size_t s);
void *xrealloc(void *p, size_t n);
void xfree(void *p);

#endif
