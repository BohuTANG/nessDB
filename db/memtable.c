/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "memtable.h"
#include "xmalloc.h"
#include "debug.h"

struct memtable *mtb_new(struct options *opts, uint64_t msn)
{
	struct memtable *mtb;
	
	mtb = xcalloc(1, sizeof(*mtb));
	mtb->msn = msn;
	mtb->opts = opts;
	mtb->bsm = basement_new();
	mtb->lgw = logw_open(opts, msn);
	if (mtb->lgw == NULL) {
		__ERROR("memtable log new error, msn [%" PRIu64 "]",
				msn);
	}

	return mtb;
}

int mtb_put(struct memtable *mtb,
		struct msg *k,
		struct msg *v,
		msgtype_t type)
{
	if (mtb->opts->enable_redo_log == 1)
		logw_append(mtb->lgw, k, v, type, 0U);
	basement_put(mtb->bsm, k, v, type);

	return NESS_OK;
}

uint64_t mtb_memusage(struct memtable *mtb)
{
	return basement_memsize(mtb->bsm);
}

void mtb_free(struct memtable *mtb)
{
	basement_free(mtb->bsm);
	logw_close(mtb->lgw);

	char name[FILE_NAME_MAXLEN];
	memset(name, 0, FILE_NAME_MAXLEN);
	snprintf(name, FILE_NAME_MAXLEN, "%s/ness.redo.%" PRIu64,
			mtb->opts->redo_path, mtb->msn);
	remove(name);
	xfree(mtb);
}
