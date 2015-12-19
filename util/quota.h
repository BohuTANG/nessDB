/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_QUOTA_H_
#define nessDB_QUOTA_H_

typedef enum quota_state {
	QUOTA_STATE_NONE,
	QUOTA_STATE_NEED_EVICT,
	QUOTA_STATE_MUST_EVICT,
} QUOTA_STATE;

struct quota {
	int waits;
	uint64_t limit;
	uint64_t used;
	uint64_t hw_value;
	ness_cond_t cond;
	ness_mutex_t lock;
};

struct quota *quota_new(uint64_t limit);
void quota_free(struct quota *q);
int quota_add(struct quota *q, uint64_t v);
int quota_remove(struct quota *q, uint64_t v);
int quota_wait(struct quota *q);
int quota_signal(struct quota *q);
QUOTA_STATE quota_state(struct quota *q);

int quota_change_limit(struct quota *q, uint64_t limit);
#endif /* nessDB_QIUOTA_H_ */


