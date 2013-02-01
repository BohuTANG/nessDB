#ifndef __nessDB_INDEX_H
#define __nessDB_INDEX_H

#include "internal.h"
#include "meta.h"
#include "buffer.h"
#include "quicklz.h"

enum QLZ_FLAG {UNCOMPRESS = 0, COMPRESS = 1};
enum OPT_FLAG {DEL = 0, ADD = 1};

struct ness_kv {
	struct slice sk;
	struct slice sv;
};

struct parking {
	int lsn;
	int async;
	struct sst *merging_sst;
};

struct index {
	int fd;
	int lsn;
	int read_fd;
	uint64_t db_alloc;
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

struct index *index_new(const char *path, struct stats *stats);
int index_add(struct index *idx, struct slice *sk, struct slice *sv);
int index_get(struct index *idx, struct slice *sk, struct slice *sv);
int index_exists(struct index *idx, struct slice *sk);
int index_remove(struct index *idx, struct slice *sk);
char *index_read_data(struct index *idx, struct ol_pair *pair);
void index_free(struct index *idx);

#endif
