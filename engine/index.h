#ifndef __nessDB_INDEX_H
#define __nessDB_INDEX_H

#include "internal.h"
#include "db.h"
#include "meta.h"
#include "buffer.h"
#include "quicklz.h"

typedef enum {UNCOMPRESS = 0, COMPRESS = 1} QLZ_FLAG;
typedef enum {DEL = 0, ADD = 1} OPT_FLAG;

struct parking {
	int lsn;
	struct sst *merging_sst;
};

struct index{
	int fd;
	int lsn;
	int read_fd;
	int max_mtb_size;
	uint64_t db_alloc;
	volatile int bg_exit;
	char tower_file[NESSDB_PATH_SIZE];
	char path[NESSDB_PATH_SIZE];

	struct meta *meta;
	struct buffer *buf;
	struct stats *stats;
	struct sst *sst;
	struct parking park;
	qlz_state_compress enstate;
	qlz_state_decompress destate;

	pthread_attr_t attr;
	pthread_mutex_t *merge_lock;
};

struct index *index_new(const char *path, int mtb_size, struct stats *stats);
int index_add(struct index *idx, struct slice *sk, struct slice *sv);
int index_get(struct index *idx, struct slice *sk, struct slice *sv);
int index_remove(struct index *idx, struct slice *sk);
char *index_read_data(struct index *idx, struct ol_pair *pair);
void index_free(struct index *idx);

#endif
