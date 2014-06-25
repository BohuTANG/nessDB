/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "file.h"
#include "mb.h"
#include "nmb.h"
#include "lmb.h"
#include "msgpack.h"
#include "compress/compress.h"
#include "layout.h"
#include "crc32.h"

/**********************************************************************
 *
 *  tree node serialize
 *
 **********************************************************************/

int _nonleaf_mb_to_packer(struct nmb *mb, struct msgpack *packer)
{
	struct mb_iter iter;

	nmb_iter_init(&iter, mb);
	nmb_iter_seektofirst(&iter);
	while (nmb_iter_valid(&iter)) {
		if (!msgpack_pack_mbiter(packer, &iter)) goto ERR;
		nmb_iter_next(&iter);
	}

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int _nonleaf_packer_to_mb(struct msgpack *packer, struct nmb *mb)
{
	while (packer->SEEK < packer->NUL) {
		struct mb_iter iter;

		if (!msgpack_unpack_mbiter(packer, &iter)) goto ERR;
		nmb_put(mb, iter.msn, iter.type, &iter.key, &iter.val, &iter.xidpair);
	}

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int _leaf_mb_to_packer(struct lmb *mb, struct msgpack *packer)
{
	struct mb_iter iter;

	lmb_iter_init(&iter, mb);
	lmb_iter_seektofirst(&iter);
	while (lmb_iter_valid(&iter)) {
		if (!msgpack_pack_mbiter(packer, &iter)) goto ERR;
		lmb_iter_next(&iter);
	}

	return NESS_OK;

ERR:
	return NESS_ERR;
}

int _leaf_packer_to_mb(struct msgpack *packer, struct lmb *mb)
{
	while (packer->SEEK < packer->NUL) {
		struct mb_iter iter;

		if (!msgpack_unpack_mbiter(packer, &iter)) goto ERR;
		lmb_put(mb, iter.msn, iter.type, &iter.key, &iter.val, &iter.xidpair);
	}

	return NESS_OK;

ERR:
	return NESS_ERR;
}

void _pack_base(struct msgpack *packer,
                char *uncompress_ptr,
                uint32_t uncompress_size,
                ness_compress_method_t compress_method)
{
	uint32_t compress_size = 0U;
	char *compress_ptr = NULL;

	/*
	 * a) magicnum
	 */
	msgpack_pack_nstr(packer, "xxxxoooo", 8);
	if (uncompress_size > 0) {
		uint32_t bound;

		bound = ness_compress_bound(compress_method, uncompress_size);
		compress_ptr =  xcalloc(1, bound);
		ness_compress(compress_method,
		              uncompress_ptr,
		              uncompress_size,
		              compress_ptr,
		              &compress_size);
		/*
		 * b) |compress_size|uncompress_size|compress datas|
		 */
		msgpack_pack_uint32(packer, compress_size);
		msgpack_pack_uint32(packer, uncompress_size);
		msgpack_pack_nstr(packer, compress_ptr, compress_size);
		xfree(compress_ptr);
	} else {
		/*
		 * b') |0|0|
		 */
		msgpack_pack_uint32(packer, 0U);
		msgpack_pack_uint32(packer, 0U);
	}

	/*
	 * c) xsum
	 */
	uint32_t xsum = 0;

	do_xsum(packer->base, packer->NUL, &xsum);
	msgpack_pack_uint32(packer, xsum);
}

void _unpack_base(struct msgpack *packer,
                  struct msgpack **dest)
{
	char *datas = NULL;
	char *new_base = NULL;
	uint32_t act_xsum;
	uint32_t exp_xsum;
	uint32_t compress_size;
	uint32_t uncompress_size;

	/* SEEK maybe not 0 */
	new_base = packer->base + packer->SEEK;
	if (!msgpack_skip(packer, 8)) goto ERR; /* magic num */

	if (!msgpack_unpack_uint32(packer, &compress_size)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &uncompress_size)) goto ERR;
	if (!msgpack_unpack_nstr(packer, compress_size, &datas)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &exp_xsum)) goto ERR;
	if (!do_xsum(new_base,
	             + 8
	             + sizeof(compress_size)
	             + sizeof(uncompress_size)
	             + compress_size,
	             &act_xsum))
		goto ERR;

	if (exp_xsum != act_xsum) {
		__ERROR("leaf xsum check error, "
		        "exp_xsum: [%" PRIu32 "], "
		        "act_xsum:[%" PRIu32 "]",
		        exp_xsum, act_xsum);
		goto ERR;
	}

	struct msgpack *dest_packer = msgpack_new(uncompress_size);

	if (uncompress_size > 0)
		ness_decompress(datas, compress_size, dest_packer->base, uncompress_size);

	dest_packer->NUL = uncompress_size;
	*dest = dest_packer;
	return;

ERR:
	*dest = NULL;
}

/*
 * serialize the leaf to buf
 */
void _serialize_leaf_to_packer(struct msgpack *packer,
                               struct node *node,
                               struct hdr *hdr)
{
	struct msgpack *lb_packer;

	lb_packer = msgpack_new(1 << 20);	/* 1MB */
	_leaf_mb_to_packer(node->u.l.buffer, lb_packer);
	_pack_base(packer, lb_packer->base, lb_packer->NUL, hdr->opts->compress_method);
	msgpack_free(lb_packer);
}

int _deserialize_leaf_from_disk(int fd,
                                struct block_pair *bp,
                                struct node **node,
                                struct status *status)
{
	int r = NESS_OK;
	uint32_t read_size;
	struct msgpack *lb_packer;
	struct msgpack *read_packer;
	struct node *n;

	read_size = ALIGN(bp->real_size);
	read_packer = msgpack_new(read_size);
	if (ness_os_pread(fd, read_packer->base, read_size, bp->offset) != (ssize_t)read_size) {
		r = NESS_READ_ERR;
		goto RET;
	}
	status_increment(&status->leaf_read_from_disk_nums);
	_unpack_base(read_packer, &lb_packer);

	/* alloc leaf node */
	n = leaf_alloc_empty(bp->nid);
	leaf_alloc_buffer(n);

	r = _leaf_packer_to_mb(lb_packer, n->u.l.buffer);
	msgpack_free(lb_packer);

	*node = n;
RET:
	msgpack_free(read_packer);
	return r;
}

void _serialize_nonleaf_to_packer(struct msgpack *node_packer,
                                  struct node *node,
                                  struct hdr *hdr,
                                  uint32_t *skeleton_size)
{
	uint32_t i;
	uint32_t part_off = 0U;
	uint32_t nchildren = 0U;

	struct msgpack *pivots_packer;
	struct msgpack *pivot_packer;

	struct msgpack *parts_packer;
	struct msgpack *part_packer;
	struct msgpack *nb_packer;

	nchildren = node->u.n.n_children;
	nassert(nchildren > 0);

	pivots_packer = msgpack_new(1 << 10);
	pivot_packer = msgpack_new(1 << 10);

	parts_packer = msgpack_new(1 << 20);
	part_packer = msgpack_new(1 << 20);
	nb_packer = msgpack_new(1 << 20);

	for (i = 0; i < nchildren; i++) {
		/* pack a part to packer */
		msgpack_clear(nb_packer);
		msgpack_clear(part_packer);
		if (node->u.n.parts[i].buffer)
			_nonleaf_mb_to_packer(node->u.n.parts[i].buffer, nb_packer);

		_pack_base(part_packer, nb_packer->base, nb_packer->NUL, hdr->opts->compress_method);

		/*
		 * pack to parts
		 * parts layout:
		 * 		|part0|
		 * 		|part1|
		 * 		|partN|
		 */
		msgpack_pack_nstr(parts_packer, part_packer->base, part_packer->NUL);

		/*
		 * pack a pivot
		 * pivot layout:
		 * 		|pivot-length|pivot-value|
		 * 		|child-nid|
		 * 		|partition-inner-offset|
		 * 		... ...
		 */
		if (i < (nchildren - 1))
			msgpack_pack_msg(pivot_packer, &node->u.n.pivots[i]);
		msgpack_pack_uint64(pivot_packer, node->u.n.parts[i].child_nid);
		/* TODO: alignment */
		msgpack_pack_uint32(pivot_packer, part_off);
		part_off = parts_packer->NUL;
	}
	msgpack_free(nb_packer);

	/*
	 * pack pivots to buffer
	 */
	_pack_base(pivots_packer,
	           pivot_packer->base,
	           pivot_packer->NUL,
	           hdr->opts->compress_method);

	/* A) pack header to buffer */
	msgpack_pack_nstr(node_packer, "nessnode", 8);
	msgpack_pack_uint32(node_packer, node->height);
	msgpack_pack_uint32(node_packer, node->u.n.n_children);

	/* B) pack pivots-buffer to buffer and do a xsum */
	uint32_t xsum = 0;

	msgpack_pack_nstr(node_packer, pivots_packer->base, pivots_packer->NUL);
	do_xsum(node_packer->base, node_packer->NUL, &xsum);
	msgpack_pack_uint32(node_packer, xsum);
	*skeleton_size = node_packer->NUL;
	msgpack_free(pivot_packer);
	msgpack_free(pivots_packer);

	/* C) pack partitions to buffer */
	msgpack_pack_nstr(node_packer, parts_packer->base, parts_packer->NUL);
	msgpack_free(part_packer);
	msgpack_free(parts_packer);

	/* D) do node xsum */
	do_xsum(node_packer->base, node_packer->NUL, &xsum);
	msgpack_pack_uint32(node_packer, xsum);
}

int _deserialize_nonleaf_from_packer(struct msgpack *packer,
                                     struct block_pair *bp,
                                     struct node **n,
                                     int light)
{
	uint32_t i;
	int r = NESS_OK;
	uint32_t height;
	uint32_t children;
	struct node *node = NULL;

	/* a) unpack magic, heigh, children */
	if (!msgpack_seek(packer, 8U)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &height)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &children)) goto ERR;

	/* new nonleaf node */
	node = nonleaf_alloc_empty(bp->nid, height, children);
	nonleaf_alloc_buffer(node);

	/* b) unpack pivots */
	struct msgpack *pivots_packer;

	_unpack_base(packer, &pivots_packer);
	for (i = 0; i < node->u.n.n_children; i++) {
		if (i < (node->u.n.n_children - 1))
			if (!msgpack_unpack_msg(pivots_packer, &node->u.n.pivots[i])) goto ERR1;
		if (!msgpack_unpack_uint64(pivots_packer, &node->u.n.parts[i].child_nid)) goto ERR1;
		if (!msgpack_unpack_uint32(pivots_packer, &node->u.n.parts[i].inner_offset)) goto ERR1;
		node->u.n.parts[i].inner_offset += bp->skeleton_size;
	}
	msgpack_free(pivots_packer);

	/* c) unpack part */
	if (!msgpack_seek(packer, bp->skeleton_size)) goto ERR1;
	if (!light) {
		for (i = 0; i < node->u.n.n_children; i++) {
			struct msgpack *lb_packer;
			uint32_t inneroff = node->u.n.parts[i].inner_offset;

			if (!msgpack_seek(packer, inneroff)) goto ERR1;
			_unpack_base(packer, &lb_packer);
			_nonleaf_packer_to_mb(lb_packer, node->u.n.parts[i].buffer);
			msgpack_free(lb_packer);
		}
	}
	*n = node;

	return NESS_OK;

ERR:
	xfree(node);

	return r;

ERR1:
	msgpack_free(pivots_packer);
	xfree(node);

	return r;
}

int _deserialize_nonleaf_from_disk(int fd,
                                   struct block_pair *bp,
                                   struct node **node,
                                   int light,
                                   struct status *status)
{
	int r = NESS_ERR;
	uint32_t read_size;
	uint32_t real_size;
	struct msgpack *read_packer = NULL;

	real_size = (light == 1) ? (bp->skeleton_size) : (bp->real_size);
	read_size = ALIGN(real_size);
	read_packer = msgpack_new(read_size);
	if (ness_os_pread(fd, read_packer->base, read_size, bp->offset) != (ssize_t)read_size) {
		r = NESS_READ_ERR;
		goto ERR;
	}
	status_increment(&status->nonleaf_read_from_disk_nums);

	/*
	 * check the checksum
	 */
	if (!msgpack_skip(read_packer, real_size - CRC_SIZE)) goto ERR;

	uint32_t exp_xsum;
	uint32_t act_xsum;

	if (!msgpack_unpack_uint32(read_packer, &exp_xsum)) goto ERR;
	if (!do_xsum(read_packer->base, real_size - CRC_SIZE, &act_xsum)) goto ERR;
	if (exp_xsum != act_xsum) {
		__ERROR("nonleaf xsum check error, "
		        "exp_xsum [%" PRIu32 "], act_xsum [%" PRIu32 "], "
		        "read size [%" PRIu32 "], nid [%" PRIu64 "]",
		        exp_xsum, act_xsum,
		        real_size, bp->nid);
		r = NESS_INNER_XSUM_ERR;
		goto ERR;
	}

	r = _deserialize_nonleaf_from_packer(read_packer, bp, node, light);
	if (r != NESS_OK) {
		__ERROR("%s", "de nonleaf from buf error");
		goto ERR;
	}
	msgpack_free(read_packer);

	return r;

ERR:
	msgpack_free(read_packer);

	return r;
}

int serialize_node_to_disk(int fd,
                           struct block *block,
                           struct node *node,
                           struct hdr *hdr)
{
	int r;
	DISKOFF address;
	uint32_t real_size;
	uint32_t skeleton_size = 0;
	struct msgpack *packer;

	packer = msgpack_new(1 << 20);
	if (node->height == 0)
		_serialize_leaf_to_packer(packer, node, hdr);
	else
		_serialize_nonleaf_to_packer(packer, node, hdr, &skeleton_size);

	real_size = packer->NUL;
	address = block_alloc_off(block, node->nid, real_size, skeleton_size, node->height);

	msgpack_pack_null(packer, ALIGN(real_size) - real_size);
	if (ness_os_pwrite(fd, packer->base, ALIGN(real_size), address) != 0) {
		r = NESS_WRITE_ERR;
		__ERROR("--write node to disk error, fd %d, "
		        "align size [%" PRIu32 "]",
		        fd,
		        ALIGN(real_size));
		goto ERR;
	}

	msgpack_free(packer);

	return NESS_OK;

ERR:
	msgpack_free(packer);

	return r;
}

int deserialize_node_from_disk(int fd,
                               struct block *block,
                               NID nid,
                               struct node **node,
                               int light)
{
	int r;
	struct block_pair *bp;

	r = block_get_off_bynid(block, nid, &bp);
	if (r != NESS_OK)
		return NESS_BLOCK_NULL_ERR;

	if (bp->height == 0)
		r = _deserialize_leaf_from_disk(fd, bp, node, block->status);
	else
		r = _deserialize_nonleaf_from_disk(fd, bp, node, light, block->status);

	return r;
}
