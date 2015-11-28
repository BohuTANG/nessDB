/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_ROLLTREE_H_
#define nessDB_ROLLTREE_H_

/**
 *
 * @file rolltree.h
 * @brief a list-tree for persisting rollback entry on dis
 *
 */

struct rolltree {
	int fd;
	struct hdr *hdr;
	struct cache_file *cf;
};

struct rolltree *rolltree_open(const char *dbname, struct cache *cache);
void rolltree_free(struct rolltree *t);

int rolltree_put(struct rolltree *t,
                 struct msg *k,
                 struct msg *v,
                 msgtype_t type,
                 TXN *txn);

#endif /* nessDB_ROLLTREE_H_ */
