/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#ifndef nessDB_QUOTA_H_
#define nessDB_QUOTA_H_

struct quota {
	int waits;
	uint64_t limit;
	uint64_t used;
	double hw_percent;
	uint64_t hw_value;
	ness_cond_t cond;
	ness_mutex_t lock;
};

struct quota *quota_new(uint64_t limit, double highwater_percent);
void quota_free(struct quota *q);
int quota_add(struct quota *q, uint64_t v);
int quota_remove(struct quota *q, uint64_t v);
int quota_check_maybe_wait(struct quota *q);
int quota_fullwater(struct quota *q);
int quota_highwater(struct quota *q);
int quota_change_limit(struct quota *q, uint64_t limit);
#endif /* nessDB_QIUOTA_H_ */


