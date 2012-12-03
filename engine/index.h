#ifndef __nessDB_INDEX_H
#define __nessDB_INDEX_H

#include "db.h"
#include "meta.h"
#include "buffer.h"
#include "skiplist.h"
#include "log.h"
#include "quicklz.h"

typedef enum {UNCOMPRESS = 0, COMPRESS = 1} QLZ_FLAG;
typedef enum {DEL = 0, ADD = 1} OPT_FLAG;

struct idx_park {
	int logno;
	struct skiplist *merging;
};

struct index{
	int fd;
	int read_fd;
	int max_mtb_size;
	uint64_t db_alloc;
	struct meta *meta;
	struct buffer *buf;
	struct skiplist *list;
	struct log *log;
	struct idx_park park;
	struct stats *stats;
	qlz_state_compress enstate;
	qlz_state_decompress destate;

	pthread_attr_t attr;
	pthread_mutex_t *merge_mutex;
	pthread_mutex_t *listfree_mutex;
};

struct index *index_new(const char *path, int mtb_size, struct stats *stats);
STATUS index_add(struct index *idx, struct slice *sk, struct slice *sv);
STATUS index_get(struct index *idx, struct slice *sk, struct slice *sv);
STATUS index_remove(struct index *idx, struct slice *sk);
struct slice *index_scan(struct index *idx, struct slice *start, struct slice *end,
						 int *ret_c);
void index_free(struct index *idx);

#endif
