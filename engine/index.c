/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/sst-algorithms.txt
 */

#include "index.h"
#include "sst.h"
#include "hashs.h"
#include "debug.h"
#include "xmalloc.h"

#define DB_MAGIC (20121212)
#define NESSDB_TOWER_EXT (".TOWER")

void _make_towername(struct index *idx, int lsn)
{
	memset(idx->tower_file, 0, NESSDB_PATH_SIZE);
	snprintf(idx->tower_file, NESSDB_PATH_SIZE, "%s/%04d%s", 
			idx->path, 
			lsn,
			NESSDB_TOWER_EXT);
}

void *_merge_job(void *arg)
{
	int i;
	int lsn;
	int async;
	int c = 0;

	struct sst_item *items;
	struct meta_node *node;

	struct sst *sst;
	struct index *idx = (struct index *)arg;

	pthread_mutex_lock(idx->merge_lock);

	sst = idx->park.merging_sst;
	lsn = idx->park.lsn;
	async = idx->park.async;

	items = sst_in_one(sst, &c);
	for (i = 0; i < c; i++) {
		node = meta_get(idx->meta, items[i].data);
		sst_add(node->sst, &items[i]);
	}
	xfree(items);

	/* remove merged TOWER */
	_make_towername(idx, lsn);
	remove(idx->tower_file);

	idx->park.merging_sst = NULL;
	sst_free(sst);

	pthread_mutex_unlock(idx->merge_lock);

	if (async) {
		pthread_detach(pthread_self());
		pthread_exit(NULL);
	} else
		return NULL;
}

void _build_tower(struct index *idx)
{
	int lsn;
	int i = 0;
	int max = 0;

	DIR *dd;
	struct sst *sst;
	struct dirent *de;
	char tower_name[NESSDB_PATH_SIZE];

	dd = opendir(idx->path);
	while ((de = readdir(dd))) {
		if (strstr(de->d_name, NESSDB_TOWER_EXT)) {
			memset(tower_name, 0, NESSDB_PATH_SIZE);
			memcpy(tower_name, de->d_name, strlen(de->d_name) - strlen(NESSDB_TOWER_EXT));
			lsn = atoi(tower_name);
			max = lsn > max ? lsn : max;
			i++;

			if (i > 2) 
				__PANIC("...TOWERS must be <= 2, pls check!");
		}
	}
	closedir(dd);

	/* redo older tower */
	for (; i > 0; i--) {
		lsn = (max - i + 1);
		_make_towername(idx, lsn);
		sst = sst_new(idx->tower_file, idx->stats);
		pthread_mutex_lock(idx->merge_lock);
		idx->park.lsn = lsn;
		idx->park.async = 0;
		idx->park.merging_sst = sst;
		pthread_mutex_unlock(idx->merge_lock);
		_merge_job(idx);
	}
}

void _check(struct index *idx)
{
	if (idx->sst->willfull) {
		pthread_mutex_lock(idx->merge_lock);
		idx->park.lsn = idx->lsn;
		idx->park.async = 1;
		idx->park.merging_sst = idx->sst;
		pthread_mutex_unlock(idx->merge_lock);

		pthread_t tid;
		pthread_create(&tid, &idx->attr, _merge_job, idx);

		_make_towername(idx, ++idx->lsn);
		idx->sst = sst_new(idx->tower_file, idx->stats);
	}
}

uint64_t _wasted(struct index *idx) 
{
	int i;
	uint64_t wasted = 0;

	for (i = 0; i < idx->meta->size; i++) 
		wasted += idx->meta->nodes[i].sst->header.wasted;

	return wasted;
}

char *index_read_data(struct index *idx, struct ol_pair *pair)
{
	int res;
	int pos = pair->offset;
	char *data = NULL;

	if (pair->offset > 0UL && pair->vlen > 0) {
		char iscompress = 0;
		uint16_t crc = 0;
		uint16_t db_crc = 0;
		char *dest = NULL;


		/* read compress flag */
		res = pread(idx->read_fd, &iscompress, sizeof(char), pos);
		if (res == -1) {
			__ERROR("read iscompress flag error");

			goto RET;
		}
		pos += sizeof(char);

		/* read crc flag */
		res = pread(idx->read_fd, &crc, sizeof(crc), pos);
		if (res == -1) {
			__ERROR("read crc error #%u", 
					crc);

			goto RET;
		}
		pos += sizeof(crc);

		/* read data */
		data = xcalloc(1, pair->vlen + 1);
		res = pread(idx->read_fd, data, pair->vlen, pos);
		if (res == -1) {
			__ERROR("read data error");

			goto RET;
		}

		db_crc = _crc16(data, pair->vlen);
		if (crc != db_crc) {
			idx->stats->STATS_CRC_ERRS++;
			__ERROR("read data crc#[%u-%u], data:len[%u], datas:[%s]", 
					crc, 
					db_crc, 
					pair->vlen,
					data);

			goto RET;
		}

		/* decompressed */
		if (iscompress) {
			int vsize = qlz_size_decompressed(data);

			dest = xcalloc(1, vsize);
			pair->vlen = qlz_decompress(data, dest, &idx->destate);
			xfree(data);

			data = dest;
		} 
	}

RET:
	return data;
}

struct index *index_new(const char *path, struct stats *stats)
{
	int magic;
	char db_name[NESSDB_PATH_SIZE];
	struct index *idx = xcalloc(1, sizeof(struct index));

	idx->stats = stats;
	idx->meta = meta_new(path, stats);
	idx->buf = buffer_new(NESSDB_MAX_VAL_SIZE);

	memcpy(idx->path, path, strlen(path));
	memset(db_name, 0, NESSDB_PATH_SIZE);
	snprintf(db_name, NESSDB_PATH_SIZE, "%s/%s", path, NESSDB_DB);

	idx->fd = n_open(db_name, N_OPEN_FLAGS, 0644);
	if (idx->fd == -1) {
		idx->fd = n_open(db_name, N_CREAT_FLAGS, 0644);
		if (idx->fd == -1) 
			__PANIC("db error, name#%s", 
					db_name);

		magic = DB_MAGIC;
		if (write(idx->fd, &magic, sizeof(magic)) < 0)
			__PANIC("write db magic error");
		idx->db_alloc += sizeof(magic);
	}

	idx->read_fd = n_open(db_name, N_OPEN_FLAGS);
	idx->db_alloc = n_lseek(idx->fd, 0, SEEK_END);

	/* DB wasted ratio */
	stats->STATS_DB_WASTED = (double) (_wasted(idx)/idx->db_alloc);

	memset(&idx->enstate, 0, sizeof(qlz_state_compress));
	memset(&idx->destate, 0, sizeof(qlz_state_decompress));

	idx->merge_lock = xmalloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(idx->merge_lock, NULL);

	/* build tower */
	_build_tower(idx);

	/* new tower file */
	_make_towername(idx, idx->lsn);
	idx->sst = sst_new(idx->tower_file, idx->stats);

	/* Detached thread attr */
	pthread_attr_init(&idx->attr);
	pthread_attr_setdetachstate(&idx->attr, PTHREAD_CREATE_DETACHED);

	return idx;
}

int index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	int ret;
	int buff_len;
	int val_len;

	uint64_t offset;

	char *line;
	struct sst_item item;

	if (sk->len >= NESSDB_MAX_KEY_SIZE || 
			sk->len < 0) {
		__ERROR("key error...#%d", 
				sk->len);

		return 0;
	}

	if (sv) {
		if (sv->len > NESSDB_MAX_VAL_SIZE ||
				sv->len < 0)
			__ERROR("value error...#%d", 
					sv->len);
	}

	/* checking */
	_check(idx);

	offset = idx->db_alloc;
	memset(&item, 0, ITEM_SIZE);
	memcpy(item.data, sk->data, sk->len);
	if (sv) {
		idx->stats->STATS_WRITES++;

		val_len = sv->len;
		/* compressed */
		if (sv->len >= NESSDB_COMPRESS_LIMIT) {
			char *dest = xcalloc(1, sv->len + 400);
			int qsize = qlz_compress(sv->data, dest, sv->len, &idx->enstate);

			buffer_putc(idx->buf, COMPRESS);
			buffer_putshort(idx->buf, _crc16(dest, qsize));
			buffer_putnstr(idx->buf, dest, qsize);
			val_len = qsize;
			xfree(dest);
			idx->stats->STATS_COMPRESSES++;
		} else {
			buffer_putc(idx->buf, UNCOMPRESS);
			buffer_putshort(idx->buf, _crc16(sv->data, sv->len));
			buffer_putnstr(idx->buf, sv->data, sv->len);
			val_len = sv->len;
		}

		buff_len = idx->buf->NUL;
		line = buffer_detach(idx->buf);

		ret = n_pwrite(idx->fd, line, buff_len, idx->db_alloc);
		if (ret == -1) 
			__PANIC("write db error");

		idx->db_alloc += buff_len;

		item.offset = offset;
		item.vlen = val_len;
		item.opt = (sv == NULL ? DEL : ADD);
	}

	sst_add(idx->sst, &item);

	return 1;
}

int index_get(struct index *idx, struct slice *sk, struct slice *sv) 
{
	struct ol_pair pair;
	struct meta_node *node;

	if (sk->len >= NESSDB_MAX_KEY_SIZE ||
			sk->len < 0) {
		__ERROR("key length error#%d", 
				sk->len);

		return 0;
	}

	idx->stats->STATS_READS++;
	memset(&pair, 0, sizeof(pair));

	/* get from TOWERs */
	if (idx->sst) {
		if (!sst_get(idx->sst, sk, &pair)) {
			if (idx->park.merging_sst)
				sst_get(idx->park.merging_sst, sk, &pair); /* need locks */
		}
	}

	if (pair.offset == 0UL) {
		node =  meta_get(idx->meta, sk->data);
		if (node) {
			if (bloom_get(node->sst->bf, sk->data)) {
				idx->stats->STATS_R_BF++;
			} else {
				idx->stats->STATS_R_NOTIN_BF++;
				goto RET;
			}

			if (sst_get(node->sst, sk, &pair)) {
				idx->stats->STATS_R_COLA++;
			} else {
				idx->stats->STATS_R_NOTIN_COLA++;
				goto RET;
			}
		}
	}

	char *data = index_read_data(idx, &pair);
	if (data) {
		sv->data = data;
		sv->len = pair.vlen;

		return 1;
	}

RET:
	return 0;
}

int index_remove(struct index *idx, struct slice *sk)
{
	if (sk->len >= NESSDB_MAX_KEY_SIZE ||
			sk->len < 0) {
		__ERROR("key length error#%d", 
				sk->len);

		return 0;
	}


	idx->stats->STATS_REMOVES++;
	return index_add(idx, sk, NULL);
}

void _flush_index(struct index *idx)
{
	__DEBUG("begin to flush TOWER to disk...");

	pthread_mutex_lock(idx->merge_lock);
	pthread_mutex_unlock(idx->merge_lock);

	/* merge TOWER to SST */
	_build_tower(idx);
}

void index_free(struct index *idx)
{
	_flush_index(idx);
	meta_free(idx->meta);
	buffer_free(idx->buf);

	pthread_mutex_unlock(idx->merge_lock);
	pthread_mutex_destroy(idx->merge_lock);
	xfree(idx->merge_lock);

	if (idx->fd > 0)
		fsync(idx->fd);

	if (idx->sst)
		sst_free(idx->sst);
	xfree(idx);
}

/*
 * [start, end): 
 */
struct ness_kv *index_scan(struct index *idx, 
			   struct slice *start, struct slice *end, 
			   int limit, int *c)
{
	int i;
	int k;
	int ms = 0;
	int ret = 0;
	struct ol_pair pair;
	struct sst_item *items = NULL;
	struct ness_kv *kv = xcalloc(1, sizeof(struct ness_kv));
	struct meta_node *nodes = meta_scan(idx->meta, 
					    start->data, end->data, 
					    &ms);

	for (i = 0; i < ms; i++) {
		int itms = 0;

		if (!nodes[i].sst)
			return kv;

		items = sst_in_one(nodes[i].sst, &itms);
		kv = xrealloc(kv, itms * sizeof(struct ness_kv));

		for(k = 0; k < itms; k++) {
			if (ret >= limit)
				goto RET;

			int cmp_a = strcmp(items[k].data, start->data);
			int cmp_b = strcmp(items[k].data, end->data);

			if (cmp_a >= 0 && cmp_b < 0) {
				pair.offset = items[k].offset;
				pair.vlen = items[k].vlen;

				kv[ret].sv.data = index_read_data(idx, &pair);
				kv[ret].sv.len = pair.vlen;

				kv[ret].sk.data = strdup(items[k].data);
				kv[ret].sk.len = strlen(items[k].data);
				ret++;
			} 
		}

		xfree(items);
		items = NULL;
	}

RET:
	if (items)
		xfree(items);
	if (nodes)
		xfree(nodes);
	*c = ret;

	return kv;
}
