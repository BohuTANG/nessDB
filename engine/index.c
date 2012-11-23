/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/cola-algorithms.txt
 */

#include "config.h"
#include "index.h"
#include "debug.h"
#include "xmalloc.h"
#include "hashs.h"

#define DB_MAGIC (20121212)

void _merging(struct meta *meta, struct skiplist *list)
{
	struct meta_node *node;
	struct skipnode *first, *cur;

	first = list->hdr;
	cur = list->hdr->forward[0];
	while (cur != first) {
		node = meta_get(meta, cur->itm.data);
		cola_add(node->cola, &cur->itm);
		cur = cur->forward[0];
	}
}

void *_merge_job(void *arg)
{
	struct index *idx;
	struct skiplist *list;

	idx = (struct index*)arg;

#ifdef BGMERGE
	pthread_mutex_lock(idx->merge_mutex);
#endif

	list = idx->park.merging;
	idx->stats->STATS_MTBL_MERGING_COUNTS = list->count;
	if (list->count > 0)
		_merging(idx->meta, list);

#ifdef BGMERGE
	pthread_mutex_lock(idx->listfree_mutex);
#endif

	skiplist_free(list);
	idx->stats->STATS_MTBL_MERGING_COUNTS = 0UL;

#ifdef BGMERGE
	pthread_mutex_unlock(idx->listfree_mutex);
#endif

	log_remove(idx->log, idx->park.logno);

#ifdef BGMERGE
	pthread_mutex_unlock(idx->merge_mutex);
	pthread_detach(pthread_self());
	pthread_exit(NULL);
#else
	return NULL;
#endif
}

void _flush_index(struct index *idx)
{
	struct skiplist *list;

	__DEBUG("begin to flush MTBL to disk...");

	list = idx->list;
	if (list->count > 0) {
#ifdef BGMERGE
		pthread_mutex_lock(idx->merge_mutex);
#endif
		_merging(idx->meta, list);
#ifdef BGMERGE
		pthread_mutex_unlock(idx->merge_mutex);
#endif

	}
	skiplist_free(list);

	/* remove current log */
	log_remove(idx->log, idx->log->no);
}

struct index *index_new(const char *path, int mtb_size, struct stats *stats)
{
	int magic;
	char db_name[NESSDB_PATH_SIZE];
	struct index *idx = xcalloc(1, sizeof(struct index));

	idx->stats = stats;
	idx->meta = meta_new(path, stats);
	idx->buf = buffer_new(NESSDB_MAX_VAL_SIZE);

	memset(db_name, 0, NESSDB_PATH_SIZE);
	snprintf(db_name, NESSDB_PATH_SIZE, "%s/%s", path, NESSDB_DB);

	idx->fd = n_open(db_name, N_OPEN_FLAGS, 0644);
	if (idx->fd == -1) {
		idx->fd = n_open(db_name, N_CREAT_FLAGS, 0644);
		if (idx->fd == -1) 
			__PANIC("db error, name#%s", db_name);
		magic = DB_MAGIC;
		if (write(idx->fd, &magic, sizeof(magic)) < 0)
			__PANIC("write db magic error");
		idx->db_alloc += sizeof(magic);
	}

	idx->read_fd = n_open(db_name, N_OPEN_FLAGS);
	idx->db_alloc = n_lseek(idx->fd, 0, SEEK_END);
	idx->list = skiplist_new(mtb_size);
	idx->max_mtb_size = mtb_size;
	idx->log = log_new(path, idx->meta, NESSDB_IS_LOG_RECOVERY);
	memset(&idx->enstate, 0, sizeof(qlz_state_compress));
	memset(&idx->destate, 0, sizeof(qlz_state_decompress));

	idx->merge_mutex = xmalloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(idx->merge_mutex, NULL);
	idx->listfree_mutex = xmalloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(idx->listfree_mutex, NULL);

	/* Detached thread attr */
	pthread_attr_init(&idx->attr);
	pthread_attr_setdetachstate(&idx->attr, PTHREAD_CREATE_DETACHED);

	return idx;
}

STATUS index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	int ret;
	int buff_len;
	int val_len;
	uint64_t offset;
	uint64_t cpt_offset = 0;

	char *line;
	struct cola_item item;

	if (sk->len >= NESSDB_MAX_KEY_SIZE || (sv && sv->len > NESSDB_MAX_VAL_SIZE)) {
		__ERROR("key or value is too long...#%d:%d", NESSDB_MAX_KEY_SIZE, NESSDB_MAX_VAL_SIZE);
		return nERR;
	}

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
			free(dest);
			idx->stats->STATS_COMPRESSES++;
		} else {
			buffer_putc(idx->buf, UNCOMPRESS);
			buffer_putshort(idx->buf, _crc16(sv->data, sv->len));
			buffer_putnstr(idx->buf, sv->data, sv->len);
			val_len = sv->len;
		}

		buff_len = idx->buf->NUL;
		line = buffer_detach(idx->buf);

		/* too many delete holes, we need to use them */
		if (idx->meta->cpt->count > NESSDB_COMPACT_LIMIT) {
			cpt_offset = cpt_get(idx->meta->cpt, val_len);
			offset = cpt_offset;
			idx->stats->STATS_HOLE_REUSES++;
		}

		if (cpt_offset > 0) {
			ret = pwrite(idx->fd, line, buff_len, cpt_offset);
		} else {
			ret = write(idx->fd, line, buff_len);
			if (ret == -1) 
				__PANIC("write db error");

			idx->db_alloc += buff_len;
		}

		item.offset = offset;
		item.vlen = val_len;
		item.opt = (sv==NULL?DEL:ADD);
	}

	/* log append */
	log_append(idx->log, &item);

	idx->stats->STATS_MTBL_COUNTS = idx->list->count;
	if (!skiplist_notfull(idx->list)) {
#ifdef BGMERGE
		pthread_t tid;
		pthread_mutex_lock(idx->merge_mutex);
#endif

		idx->stats->STATS_MERGES++;
		idx->park.merging = idx->list;
		idx->park.logno = idx->log->no;

#ifdef BGMERGE
		pthread_mutex_unlock(idx->merge_mutex);
		pthread_create(&tid, &idx->attr, _merge_job, idx);
#else
		_merge_job((void*)idx);
#endif

		idx->list = skiplist_new(idx->max_mtb_size);
		log_create(idx->log);
	}
	skiplist_insert(idx->list, &item);

	return nOK;
}

STATUS index_get(struct index *idx, struct slice *sk, struct slice *sv) 
{
	int res;
	struct ol_pair pair;
	struct meta_node *node;
	struct skipnode *sknode;
	struct skiplist *cur_list;

	if (sk->len >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length big than MAX#%d", NESSDB_MAX_KEY_SIZE);
		return nERR;
	}

	idx->stats->STATS_READS++;
	memset(&pair, 0, sizeof pair);

	/* active memtable */
	cur_list = idx->list;
	sknode = skiplist_lookup(cur_list, sk->data);
	if (sknode) {
		if (sknode->itm.opt == ADD) {
			idx->stats->STATS_R_FROM_MTBL++;
			pair.offset = sknode->itm.offset;
			pair.vlen = sknode->itm.vlen;
		}
	} else {
#ifdef BGMERGE
		pthread_mutex_lock(idx->listfree_mutex);

		struct skiplist *merging_list = idx->park.merging;
		if (merging_list) {
			sknode = skiplist_lookup(merging_list, sk->data);
			if (sknode && sknode->itm.opt == ADD ) {
				idx->stats->STATS_R_FROM_MTBL++;
				pair.offset = sknode->itm.offset;
				pair.vlen = sknode->itm.vlen;
			}
		}
		pthread_mutex_unlock(idx->listfree_mutex);
#endif
	} 

	if (!sknode) {
		node =  meta_get(idx->meta, sk->data);
		if (node) {
			if (bloom_get(node->cola->bf, sk->data)) {
				idx->stats->STATS_R_BF++;
			} else {
				idx->stats->STATS_R_NOTIN_BF++;
				goto RET;
			}

			if (cola_get(node->cola, sk, &pair)) {
				idx->stats->STATS_R_COLA++;
			} else {
				idx->stats->STATS_R_NOTIN_COLA++;
				goto RET;
			}
		}
	}

	if (pair.offset > 0UL && pair.vlen > 0) {
		char iscompress = 0;
		short crc = 0;
		short db_crc = 0;
		char *data;
		char *dest = NULL;

		n_lseek(idx->read_fd, pair.offset, SEEK_SET);

		/* read compress flag */
		res = read(idx->read_fd, &iscompress, sizeof(char));
		if (res == -1) {
			__ERROR("read iscompress flag error");
			goto RET;
		}

		/* read crc flag */
		res = read(idx->read_fd, &crc, sizeof(crc));
		if (res == -1) {
			__ERROR("read crc error #%d, key#%s", crc, sk->data);
			goto RET;
		}

		/* read data */
		data = xcalloc(1, pair.vlen + 1);
		res = read(idx->read_fd, data, pair.vlen);
		if (res == -1) {
			__ERROR("read data error, key#%d", sk->data);
			goto RET;
		}

		/* decompressed */
		if (iscompress) {
			int vsize = qlz_size_decompressed(data);

			dest = xcalloc(1, vsize);
			pair.vlen = qlz_decompress(data, dest, &idx->destate);
			free(data);

			data = dest;
		} 

		db_crc = _crc16(data, pair.vlen);
		if (crc != db_crc) {
			idx->stats->STATS_CRC_ERRS++;
			__ERROR("read key#%s, crc#%d, db_crc#%d, data [%s]", sk->data, crc, db_crc, data);
			goto RET;
		}

		sv->data = data;
		sv->len = pair.vlen;

		return nOK;
	}

RET:
	return nERR;
}

STATUS index_remove(struct index *idx, struct slice *sk)
{
	if (sk->len >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length big than MAX#%d", NESSDB_MAX_KEY_SIZE);
		return nERR;
	}

	idx->stats->STATS_REMOVES++;
	return index_add(idx, sk, NULL);
}

void index_free(struct index *idx)
{
	_flush_index(idx);
	free(idx->merge_mutex);
	free(idx->listfree_mutex);
	meta_free(idx->meta);
	buffer_free(idx->buf);
	log_free(idx->log);
	free(idx);
}
