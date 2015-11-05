/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
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
	struct env *e;
};

struct rolltree *rolltree_open(const char *dbname, struct env *e);
void rolltree_free(struct rolltree *t);

int rolltree_put(struct rolltree *t,
             struct msg *k,
             struct msg *v,
             msgtype_t type,
             TXN *txn);

#endif /* nessDB_ROLLTREE_H_ */
