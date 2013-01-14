/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "meta.h"
#include "debug.h"
#include "xmalloc.h"

void  _check_dir(const char *pathname)  
{  
	int i, len;
	char  dirname[NESSDB_PATH_SIZE];  

	strcpy(dirname, pathname);  
	i = strlen(dirname);  
	len = i;

	if (dirname[len-1] != '/')  
		strcat(dirname,  "/");  

	len = strlen(dirname);  
	for (i = 1; i < len; i++) {  
		if (dirname[i] == '/') {  
			dirname[i] = 0;  
			if(access(dirname, 0) != 0) {  
				if(mkdir(dirname, 0755)==-1) 
					__PANIC("creta dir error, %s", 
							dirname);
			}  
			dirname[i] = '/';  
		}  
	}  
}

void _make_sstname(struct meta *meta, int lsn)
{
	memset(meta->sst_file, 0, NESSDB_PATH_SIZE);
	snprintf(meta->sst_file, NESSDB_PATH_SIZE, "%s/%06d%s", 
			meta->path, 
			lsn, 
			NESSDB_SST_EXT);
}

int _get_idx(struct meta *meta, char *key)
{
	int cmp;
	int left = 0, i;
	int right = meta->size;
	struct meta_node *node;

	while (left < right) {
		i = (right + left) / 2 ;
		node = &meta->nodes[i];

		cmp = strcmp(key, node->sst->header.max_key);
		if (cmp == 0) 
			return i;

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}

	i = left;

	return i;
}

void meta_dump(struct meta *meta)
{
	int i, j;
	int allc = 0;
	uint64_t allwasted = 0;
	
	__DEBUG("---meta(%d):", 
			meta->size);

	for (i = 0; i < meta->size; i++) {
		int used = 0;
		int count = 0;
		int wasted = meta->nodes[i].sst->header.wasted;

		for (j = 0; j < MAX_LEVEL; j++) {
			used += meta->nodes[i].sst->header.count[j] * ITEM_SIZE;
			count += meta->nodes[i].sst->header.count[j];
		}

		allc += count;
		allwasted += wasted;
		__DEBUG("\t-----[%d] #%06d.SST, max-key#%s, used#%d, hole-wasted#%d, count#%d",
				i,
				meta->nodes[i].lsn,
				meta->nodes[i].sst->header.max_key,
				used,
				wasted,
				count);
	}
	__DEBUG("\t----allcount:%d, allwasted(KB):%llu", 
			allc,
			allwasted/1024);
}

void _scryed(struct meta *meta,struct sst *sst, struct sst_item *L, int start, int c, int idx)
{
	int i;
	int k = c + start;

	for (i = start; i < k; i++) {
		if (L[i].opt & 1) {
			sst_add(sst, &L[i]);
		}
	}

	/* update new meta node */
	memmove(&meta->nodes[idx + 1], &meta->nodes[idx], (meta->size - idx) * META_NODE_SIZE);
	memset(&meta->nodes[idx], 0, sizeof(struct meta_node));
	meta->nodes[idx].sst = sst;
	meta->nodes[idx].lsn = meta->size;

	meta->size++;

	if (meta->size >= (int)(NESSDB_MAX_META - 1))
		__PANIC("OVER max metas, MAX#%d", 
				NESSDB_MAX_META);
}

void _split_sst(struct meta *meta, struct meta_node *node)
{
	int i;
	int k;
	int split;
	int mod;
	int c = 0;
	int nxt_idx;

	struct sst_item *L;
	struct sst *sst;

	L = sst_in_one(node->sst, &c);

	split = c / NESSDB_SST_SEGMENT;
	mod = c % NESSDB_SST_SEGMENT;
	k = split + mod;

	/* rewrite SST */
	nxt_idx = _get_idx(meta, L[k - 1].data) + 1;
	sst = node->sst;

	/* truncate all SST */
	sst_truncate(sst);
	memcpy(node->sst->header.max_key, L[k - 1].data, strlen(L[k - 1].data));
	for (i = 0; i < k; i++) {
		if (L[i].opt & 1) {
			sst_add(sst, &L[i]);
		}
	}

	/* others SST */
	for (i = 1; i < NESSDB_SST_SEGMENT; i++) {
		_make_sstname(meta, meta->size);
		sst = sst_new(meta->sst_file, meta->stats);
		_scryed(meta, sst, L, mod + i*split, split, nxt_idx++);
	}

	xfree(L);
	meta->stats->STATS_SST_SPLITS++;
}

void _build_meta(struct meta *meta)
{
	int idx;
	int lsn;
	DIR *dd;
	struct dirent *de;
	struct sst *sst;
	char sst_name[NESSDB_PATH_SIZE];

	dd = opendir(meta->path);
	while ((de = readdir(dd))) {
		if (strstr(de->d_name, NESSDB_SST_EXT)) {
			memset(sst_name, 0, NESSDB_PATH_SIZE);
			memcpy(sst_name, de->d_name, strlen(de->d_name) - 4);
			lsn = atoi(sst_name);
			_make_sstname(meta, lsn);
			sst = sst_new(meta->sst_file, meta->stats);

			idx = _get_idx(meta, sst->header.max_key);
			memmove(&meta->nodes[idx + 1], &meta->nodes[idx], (meta->size - idx) * META_NODE_SIZE);
			meta->nodes[idx].sst = sst;
			meta->nodes[idx].lsn = lsn;

			meta->size++;
		}
	}

	if (meta->size == 0) {
		_make_sstname(meta, 0);
		sst = sst_new(meta->sst_file, meta->stats);
		meta->nodes[0].sst = sst;
		meta->nodes[0].lsn = 0;
		meta->size++;
	}

	closedir(dd);
	meta_dump(meta);
}

struct meta *meta_new(const char *path, struct stats *stats)
{
	struct meta *m = xcalloc(1, sizeof(struct meta));

	m->stats = stats;
	memcpy(m->path, path, NESSDB_PATH_SIZE);
	_check_dir(path);
	_build_meta(m);

	return m;
}

struct meta_node *meta_get(struct meta *meta, char *key)
{
	int i;
	struct meta_node *node;

	i = _get_idx(meta, key);
	node = &meta->nodes[i];

	if (i > 0 && i == meta->size) 
		node = &meta->nodes[i - 1];

	if (node->sst->willfull) {
		_split_sst(meta, node);
		node = meta_get(meta, key);
	}

	return node;
}

/*
 * meta scan for range query
 */
struct meta_node *meta_scan(struct meta *meta, char *start, char *end,
						   int *ret_c)
{
	int s_idx = _get_idx(meta, start);
	int e_idx = _get_idx(meta, end);
	int c = e_idx - s_idx + 1;
	struct meta_node *nodes = xcalloc(c, sizeof(struct meta_node));

	*ret_c = c;
	memcpy(nodes, &meta->nodes[s_idx], c * sizeof(struct meta_node));

	return nodes;
}

void meta_free(struct meta *meta)
{
	int i;
	struct sst *sst;

	for (i = 0; i < meta->size; i++) {
		sst = meta->nodes[i].sst;
		if (sst) {
			if (sst->fd > 0)
				fsync(sst->fd);
			sst_free(sst);
		}
	}

	if (meta)
		xfree(meta);
}
