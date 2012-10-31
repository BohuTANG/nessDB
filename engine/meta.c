/*
 * Copyright (c) 2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 * The main algorithm described is here: spec/cola-algorithms.txt
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>

#include "meta.h"
#include "debug.h"
#include "xmalloc.h"
#include "sorts.h"

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
					__PANIC("creta dir error, %s", dirname);
			}  
			dirname[i] = '/';  
		}  
	}  
}

void _make_sstname(struct meta *meta, int lsn)
{
	memset(meta->sst_file, 0, NESSDB_PATH_SIZE);
	snprintf(meta->sst_file, NESSDB_PATH_SIZE, "%s/%06d.sst", meta->path, lsn);
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

		cmp = strcmp(key, node->cola->header.max_key);
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
	int i;
	
	__DEBUG("---meta(%d):", meta->size);
	for (i = 0; i < meta->size; i++)
		__DEBUG("\t-----#%06d.sst, key#%s", meta->nodes[i].lsn, meta->nodes[i].cola->header.max_key);
}

void _split_sst(struct meta *meta, struct meta_node *node)
{
	int i;
	int mid;
	int c = 0;
	int nxt_idx;

	struct cola_item *L;
	struct cola *cola;

	L = cola_in_one(node->cola, &c);

	mid = c / 2;
	i = mid + 1;

	_make_sstname(meta, meta->size);
	cola = cola_new(meta->sst_file);

	for (; i < c; i++)
		if (L[i].opt == 1) {
			cola_add(cola, &L[i]);
		}

	/* update new meta node */
	nxt_idx = _get_idx(meta, L[mid].data) + 1;

	memmove(&meta->nodes[nxt_idx + 1], &meta->nodes[nxt_idx], (meta->size - nxt_idx) * META_NODE_SIZE);
	memset(&meta->nodes[nxt_idx], 0, sizeof(struct meta_node));
	meta->nodes[nxt_idx].cola = cola;
	meta->nodes[nxt_idx].lsn = meta->size;

	meta->size++;

	if (meta->size >= (int)(NESSDB_MAX_META - 1))
		__PANIC("OVER max metas, MAX#%d", NESSDB_MAX_META);

	cola = node->cola;
	cola_truncate(cola);

	/* update max key to mid */
	memcpy(node->cola->header.max_key, L[mid].data, strlen(L[mid].data));

	for (i = 0; i <= mid; i++)
		if (L[i].opt == 1) {
			cola_add(cola, &L[i]);
		}

	__DEBUG("----------sst file splitted,count#%d", c);
	free(L);
}

void _build_meta(struct meta *meta)
{
	int idx;
	int lsn;
	DIR *dd;
	struct dirent *de;
	struct cola *cola;
	char sst_name[NESSDB_PATH_SIZE];

	dd = opendir(meta->path);
	while ((de = readdir(dd))) {
		if (strstr(de->d_name, ".sst")) {
			memset(sst_name, 0, NESSDB_PATH_SIZE);
			memcpy(sst_name, de->d_name, strlen(de->d_name) - 4);
			lsn = atoi(sst_name);
			_make_sstname(meta, lsn);
			cola = cola_new(meta->sst_file);

			idx = _get_idx(meta, cola->header.max_key);
			memmove(&meta->nodes[idx + 1], &meta->nodes[idx], (meta->size - idx) * META_NODE_SIZE);
			meta->nodes[idx].cola = cola;
			meta->nodes[idx].lsn = lsn;

			meta->size++;
		}
	}

	if (meta->size == 0) {
		_make_sstname(meta, 0);
		cola = cola_new(meta->sst_file);
		meta->nodes[0].cola = cola;
		meta->nodes[0].lsn = 0;
		meta->size++;
	}

	closedir(dd);
	meta_dump(meta);
}

struct meta *meta_new(const char *path)
{
	struct meta *m = xcalloc(1, sizeof(struct meta));

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

	if (node->cola->willfull) {
		_split_sst(meta, node);
		node = meta_get(meta, key);
	}

	return node;
}

void meta_free(struct meta *meta)
{
	int i;
	struct cola *cola;

	for (i = 0; i < meta->size; i++) {
		cola = meta->nodes[i].cola;
		if (cola) 
			cola_free(cola);
	}

	if (meta)
		free(meta);
}
