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
	if (list->count > 0)
		_merging(idx->meta, list);
	skiplist_free(list);
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

struct index *index_new(const char *path, int mtb_size)
{
	char db_name[NESSDB_PATH_SIZE];
	struct index *idx = xcalloc(1, sizeof(struct index));

	idx->meta = meta_new(path);
	idx->buf = buffer_new(5 * 1024 * 1024);

	memset(db_name, 0, NESSDB_PATH_SIZE);
	snprintf(db_name, NESSDB_PATH_SIZE, "%s/%s", path, NESSDB_DB);

	idx->fd = n_open(db_name, N_OPEN_FLAGS, 0644);
	if (idx->fd == -1) {
		idx->fd = n_open(db_name, N_CREAT_FLAGS, 0644);
		if (idx->fd == -1) 
			__PANIC("db error, name#%s", db_name);
	}

	idx->read_fd = n_open(db_name, N_OPEN_FLAGS);
	idx->db_alloc = n_lseek(idx->fd, 0, SEEK_END);
	idx->list = skiplist_new(mtb_size);
	idx->max_mtb_size = mtb_size;
	idx->log = log_new(path, idx->meta, NESSDB_IS_LOG_RECOVERY);

	idx->merge_mutex = xmalloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(idx->merge_mutex, NULL);

	/* Detached thread attr */
	pthread_attr_init(&idx->attr);
	pthread_attr_setdetachstate(&idx->attr, PTHREAD_CREATE_DETACHED);

	return idx;
}

STATUS index_add(struct index *idx, struct slice *sk, struct slice *sv)
{
	int ret;
	int len;
	char *line;
	struct cola_item item;

	if (sk->len >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length big than MAX#%d", NESSDB_MAX_KEY_SIZE);
		return nERR;
	}

	memset(&item, 0, ITEM_SIZE);
	memcpy(item.data, sk->data, sk->len);
	if (sv) {
		/* write value */
		buffer_putshort(idx->buf, _crc16(sv->data, sv->len));
		buffer_putnstr(idx->buf, sv->data, sv->len);
		len = idx->buf->NUL;
		line = buffer_detach(idx->buf);
		ret = write(idx->fd, line, len);
		if (ret == -1) 
			__PANIC("write db error");

		item.offset = idx->db_alloc;
		item.vlen = sv->len;
		item.opt = 1;

		idx->db_alloc += len;
	}

	/* log append */
	log_append(idx->log, &item);

	if (!skiplist_notfull(idx->list)) {
#ifdef BGMERGE
		pthread_t tid;
		pthread_mutex_lock(idx->merge_mutex);
#endif

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
	struct skipnode *sknode;
	struct meta_node *node;

	if (sk->len >= NESSDB_MAX_KEY_SIZE) {
		__ERROR("key length big than MAX#%d", NESSDB_MAX_KEY_SIZE);
		return nERR;
	}

	memset(&pair, 0, sizeof pair);
	sknode = skiplist_lookup(idx->list, sk->data);
	if (sknode) {
		pair.offset = sknode->itm.offset;
		pair.vlen = sknode->itm.vlen;
	} else {
		node =  meta_get(idx->meta, sk->data);
		if (node) {
			if (!bloom_get(node->cola->bf, sk->data))
				goto RET;

			if (!cola_get(node->cola, sk, &pair))
				goto RET;
		}
	}

	if (pair.offset > 0UL && pair.vlen > 0) {
		short crc = 0;
		short db_crc = 0;
		char *data;

		n_lseek(idx->read_fd, pair.offset, SEEK_SET);
		res = read(idx->read_fd, &crc, sizeof(crc));
		if (res == -1) {
			__ERROR("read crc error #%d, key#%s", crc, sk->data);
			goto RET;
		}

		data = xcalloc(1, pair.vlen + 1);
		res = read(idx->read_fd, data, pair.vlen);
		if (res == -1) {
			__ERROR("read data error, key#%d", sk->data);
			goto RET;
		}

		db_crc = _crc16(data, pair.vlen);
		if (crc != db_crc) {
			__ERROR("read key#%s, crc#%d, db_crc#%d", sk->data, crc, db_crc);
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

	return index_add(idx, sk, NULL);
}

void index_free(struct index *idx)
{
	_flush_index(idx);
	free(idx->merge_mutex);
	meta_free(idx->meta);
	buffer_free(idx->buf);
	log_free(idx->log);
	free(idx);
}
