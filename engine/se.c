/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "file.h"
#include "msgbuf.h"
#include "se.h"
#include "compress/compress.h"

/**********************************************************************
 *
 *  tree node serialize
 *
 **********************************************************************/

void _leaf_msgbuf_to_buf(struct msgbuf *mb, struct buffer *buf)
{
	struct msgbuf_iter iter;

	msgbuf_iter_init(&iter, mb);
	msgbuf_iter_seektofirst(&iter);
	while (msgbuf_iter_valid(&iter)) {
		/*
		 * if type is MSG_DEL in leaf,
		 * we don't need to write to disk
		 */
		if (iter.type != MSG_DELETE) {
			MSN msn = iter.msn;

			msn = ((msn << 8) | iter.type);
			buf_putuint64(buf, msn);
			buf_putuint64(buf, iter.xidpair.child_xid);
			buf_putuint64(buf, iter.xidpair.parent_xid);
			buf_putuint32(buf, iter.key.size);
			buf_putnstr(buf, iter.key.data, iter.key.size);

			buf_putuint32(buf, iter.val.size);
			buf_putnstr(buf, iter.val.data, iter.val.size);
		}
		msgbuf_iter_next(&iter);
	}
}

void _nonleaf_msgbuf_to_buf(struct msgbuf *mb, struct buffer *buf)
{
	struct msgbuf_iter iter;

	msgbuf_iter_init(&iter, mb);
	msgbuf_iter_seektofirst(&iter);
	while (msgbuf_iter_valid(&iter)) {
		MSN msn = iter.msn;

		msn = ((msn << 8) | iter.type);
		buf_putuint64(buf, msn);
		buf_putuint64(buf, iter.xidpair.child_xid);
		buf_putuint64(buf, iter.xidpair.parent_xid);
		buf_putuint32(buf, iter.key.size);
		buf_putnstr(buf, iter.key.data, iter.key.size);

		if (iter.type != MSG_DELETE) {
			buf_putuint32(buf, iter.val.size);
			buf_putnstr(buf, iter.val.data, iter.val.size);
		}
		msgbuf_iter_next(&iter);
	}
}

int _buf_to_msgbuf(struct buffer *rbuf, uint32_t size, struct msgbuf *mb)
{
	uint32_t pos = 0;

	while (pos < size) {
		MSN msn;
		uint8_t type;
		struct txnid_pair xidpair;
		struct msg k;
		struct msg v;

		if (!buf_getuint64(rbuf, &msn)) goto ERR;
		pos += sizeof(uint64_t);
		type = (msn & 0xff);
		msn = (msn >> 8);
		if (!buf_getuint64(rbuf, &xidpair.child_xid)) goto ERR;
		pos += sizeof(uint64_t);

		if (!buf_getuint64(rbuf, &xidpair.parent_xid)) goto ERR;
		pos += sizeof(uint64_t);

		if (!buf_getuint32(rbuf, &k.size)) goto ERR;
		pos += sizeof(uint32_t);

		if (!buf_getnstr(rbuf, k.size, (char**)&k.data)) goto ERR;
		pos += k.size;

		if (type != MSG_DELETE) {
			if (!buf_getuint32(rbuf, &v.size)) goto ERR;
			pos += sizeof(uint32_t);

			if (!buf_getnstr(rbuf, v.size, (char**)&v.data)) goto ERR;
			pos += v.size;
		}

		msgbuf_put(mb, msn, type, &k, &v, &xidpair);
	}

	return NESS_OK;

ERR:
	return NESS_ERR;
}

/*
 * serialize the leaf to buf
 */
void _serialize_leaf_to_buf(struct buffer *wbuf,
                            struct node *node,
                            struct hdr *hdr)
{
	struct buffer *buf;
	char *compress_ptr = NULL;
	char *uncompress_ptr;
	uint32_t compress_size = 0U;
	uint32_t uncompress_size;

	buf = buf_new(1 << 20);	/* 1MB */
	_leaf_msgbuf_to_buf(node->u.l.buffer, buf);
	uncompress_size = buf->NUL;
	uncompress_ptr = buf->buf;

	/*
	 * a) magicnum
	 */
	buf_putnstr(wbuf, "nodeleaf", 8);
	if (uncompress_size > 0) {
		uint32_t bound;

		bound = ness_compress_bound(hdr->method, uncompress_size);
		compress_ptr =  xcalloc(1, bound);
		ness_compress(hdr->method,
		              uncompress_ptr,
		              uncompress_size,
		              compress_ptr,
		              &compress_size);

		/*
		 * b) |compress_size|uncompress_size|compress datas|
		 */
		buf_putuint32(wbuf, compress_size);
		buf_putuint32(wbuf, uncompress_size);
		buf_putnstr(wbuf, compress_ptr, compress_size);
		xfree(compress_ptr);
	} else {
		/*
		 * b') |0|0|
		 */
		buf_putuint32(wbuf, 0U);
		buf_putuint32(wbuf, 0U);
	}
	buf_free(buf);

	/*
	 * c) xsum
	 */
	uint32_t xsum;

	buf_xsum(wbuf->buf, wbuf->NUL, &xsum);
	buf_putuint32(wbuf, xsum);
}

void _serialize_nonleaf_to_buf(struct buffer *wbuf,
                               struct node *node,
                               struct hdr *hdr,
                               uint32_t *skeleton_size)
{
	uint32_t i;
	uint32_t part_off = 0U;
	uint32_t nchildren = 0U;
	uint32_t compress_size = 0U;
	uint32_t uncompress_size;
	char *compress_ptr;
	char *uncompress_ptr;

	struct buffer *pivots_wbuf;
	struct buffer *parts_wbuf;
	struct buffer *part_wbuf;

	nchildren = node->u.n.n_children;
	if (nchildren == 0) return;

	pivots_wbuf = buf_new(1 << 10);
	parts_wbuf = buf_new(1 << 20);
	part_wbuf = buf_new(1 << 20);

	for (i = 0; i < nchildren; i++) {
		/*
		 * a) pack part entries
		 * part layout:
		 * 		|compress_size|
		 * 		|uncompress_size|
		 * 		|compress datas|
		 * 		|xsum|
		 */
		buf_clear(part_wbuf);
		if (node->u.n.parts[i].buffer)
			_nonleaf_msgbuf_to_buf(node->u.n.parts[i].buffer,
			                       part_wbuf);

		uncompress_size = part_wbuf->NUL;
		uncompress_ptr = part_wbuf->buf;

		buf_clear(part_wbuf);
		if (uncompress_size > 0) {
			uint32_t bound;

			bound = ness_compress_bound(hdr->method, uncompress_size);
			compress_ptr =  xcalloc(1, bound);
			ness_compress(hdr->method,
			              uncompress_ptr,
			              uncompress_size,
			              compress_ptr,
			              &compress_size);

			buf_putuint32(part_wbuf, compress_size);
			buf_putuint32(part_wbuf, uncompress_size);
			buf_putnstr(part_wbuf, compress_ptr, compress_size);
			xfree(compress_ptr);
		} else {
			buf_putuint32(part_wbuf, 0U);
			buf_putuint32(part_wbuf, 0U);
		}

		uint32_t xsum;

		buf_xsum(part_wbuf->buf, part_wbuf->NUL, &xsum);
		buf_putuint32(part_wbuf, xsum);


		/*
		 * b) pack part to parts buffer
		 * parts layout:
		 * 		|part0|
		 * 		|part1|
		 * 		|partN|
		 */
		buf_putnstr(parts_wbuf, part_wbuf->buf, part_wbuf->NUL);
		buf_clear(part_wbuf);

		/*
		 * c) pack pivot to pivots buffer
		 * pivot layout:
		 * 		|pivot-length|pivot-value|
		 * 		|child-nid|
		 * 		|partition-inner-offset|
		 * 		... ...
		 */
		if (i < (nchildren - 1)) {
			buf_putmsg(pivots_wbuf, &node->u.n.pivots[i]);
			buf_putuint64(pivots_wbuf, node->u.n.parts[i].child_nid);
			buf_putuint32(pivots_wbuf, part_off);
		} else {
			buf_putuint64(pivots_wbuf, node->u.n.parts[i].child_nid);
			buf_putuint32(pivots_wbuf, part_off);
		}

		part_off = parts_wbuf->NUL;
	}
	buf_free(part_wbuf);


	/*
	 * d) pack pivots to buffer
	 * pivots layout :
	 * 		|compress_size|
	 * 		|uncompress_size|
	 * 		|compress datas|
	 */
	compress_size = 0UL;
	uncompress_size = pivots_wbuf->NUL;
	nassert(uncompress_size > 0);

	compress_ptr = NULL;
	uncompress_ptr = pivots_wbuf->buf;

	uint32_t bound;

	bound = ness_compress_bound(hdr->method, uncompress_size);
	compress_ptr =  xcalloc(1, bound);
	ness_compress(hdr->method,
	              uncompress_ptr, uncompress_size,
	              compress_ptr, &compress_size);
	buf_clear(pivots_wbuf);
	buf_putuint32(pivots_wbuf, compress_size);
	buf_putuint32(pivots_wbuf, uncompress_size);
	buf_putnstr(pivots_wbuf, compress_ptr, compress_size);
	xfree(compress_ptr);

	/* A) pack header to buffer */
	buf_putnstr(wbuf, "nessnode", 8);
	buf_putuint32(wbuf, node->height);
	buf_putuint32(wbuf, node->u.n.n_children);

	/* B) pack pivots-buffer to buffer and do a xsum */
	uint32_t xsum;

	buf_putnstr(wbuf, pivots_wbuf->buf, pivots_wbuf->NUL);
	buf_xsum(wbuf->buf, wbuf->NUL, &xsum);
	buf_putuint32(wbuf, xsum);
	*skeleton_size = wbuf->NUL;
	buf_free(pivots_wbuf);

	/* C) pack partitions to buffer */
	buf_putnstr(wbuf, parts_wbuf->buf, parts_wbuf->NUL);
	buf_free(parts_wbuf);

	/* D) do node xsum */
	buf_xsum(wbuf->buf, wbuf->NUL, &xsum);
	buf_putuint32(wbuf, xsum);
}

int _deserialize_leaf_from_disk(int fd,
                                struct block_pair *bp,
                                struct node **node)
{
	int r = NESS_OK;
	uint32_t read_size;
	struct buffer *rbuf;

	read_size = ALIGN(bp->real_size);
	rbuf = buf_new(read_size);
	if (ness_os_pread(fd,
	                  rbuf->buf,
	                  read_size,
	                  bp->offset) != (ssize_t)read_size) {
		r = NESS_READ_ERR;
		goto RET;
	}

	char *datas = NULL;
	uint32_t act_xsum;
	uint32_t exp_xsum;
	uint32_t compress_size;
	uint32_t uncompress_size;

	if (!buf_skip(rbuf, 8)) goto RET; /* magic num */

	if (!buf_getuint32(rbuf, &compress_size)) goto RET;
	if (!buf_getuint32(rbuf, &uncompress_size)) goto RET;
	if (!buf_getnstr(rbuf, compress_size, &datas)) goto RET;
	if (!buf_getuint32(rbuf, &exp_xsum)) goto RET;
	if (!buf_xsum(rbuf->buf,
	              + 8
	              + sizeof(compress_size)
	              + sizeof(uncompress_size)
	              + compress_size,
	              &act_xsum))
		goto RET;

	if (exp_xsum != act_xsum) {
		__ERROR("leaf xsum check error, "
		        "exp_xsum: [%" PRIu32 "], "
		        "act_xsum:[%" PRIu32 "]",
		        exp_xsum,
		        act_xsum);
		r = NESS_LEAF_XSUM_ERR;
		goto RET;
	}

	struct node *n;
	struct buffer *lbuf;

	lbuf = buf_new(uncompress_size);
	if (uncompress_size > 0) {
		ness_decompress(datas,
		                compress_size,
		                lbuf->buf,
		                uncompress_size);
	}

	/* alloc leaf node */
	n = leaf_alloc_empty(bp->nid);
	leaf_alloc_msgbuf(n);

	r = _buf_to_msgbuf(lbuf,
	                   uncompress_size,
	                   n->u.l.buffer);
	if (r != NESS_OK) {
		__ERROR("buf to dmt error, "
		        "buf raw_size [%" PRIu32 "]",
		        uncompress_size);
	}
	buf_free(lbuf);

	*node = n;
RET:
	buf_free(rbuf);
	return r;
}

int _deserialize_nonleaf_from_buf(struct buffer *rbuf,
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
	if (!buf_seek(rbuf, 8U)) goto ERR;
	if (!buf_getuint32(rbuf, &height)) goto ERR;
	if (!buf_getuint32(rbuf, &children)) goto ERR;

	/* new nonleaf node */
	node = nonleaf_alloc_empty(bp->nid, height, children);
	nonleaf_alloc_buffer(node);

	/* b) unpack pivots */
	char *compress_ptr;
	uint32_t compress_size;
	uint32_t uncompress_size;

	if (!buf_getuint32(rbuf, &compress_size)) goto ERR;
	if (!buf_getuint32(rbuf, &uncompress_size)) goto ERR;
	if (!buf_getnstr(rbuf, compress_size, &compress_ptr)) goto ERR;

	struct buffer *pivots_rbuf = buf_new(uncompress_size);
	ness_decompress(compress_ptr,
	                compress_size,
	                pivots_rbuf->buf,
	                uncompress_size);

	for (i = 0; i < node->u.n.n_children; i++) {
		if (i < (node->u.n.n_children - 1)) {
			if (!buf_getmsg(pivots_rbuf, &node->u.n.pivots[i]))
				goto ERR1;
		}
		if (!buf_getuint64(pivots_rbuf, &node->u.n.parts[i].child_nid))
			goto ERR1;
		if (!buf_getuint32(pivots_rbuf, &node->u.n.parts[i].inner_offset))
			goto ERR1;
		node->u.n.parts[i].inner_offset += bp->skeleton_size;
	}

	/* c) unpack part */
	if (!buf_seek(rbuf, bp->skeleton_size)) goto ERR1;
	if (!light) {
		for (i = 0; i < node->u.n.n_children; i++) {
			char *part_start;
			char *part_datas;
			uint32_t exp_xsum;
			uint32_t act_xsum;
			uint32_t part_size;
			uint32_t part_raw_size;

			buf_pos(rbuf, &part_start);
			if (!buf_getuint32(rbuf, &part_size)) goto ERR1;
			if (!buf_getuint32(rbuf, &part_raw_size)) goto ERR1;
			if (!buf_getnstr(rbuf, part_size, (char**)&part_datas)) goto ERR1;
			if (!buf_getuint32(rbuf, &exp_xsum)) goto ERR1;
			if (!buf_xsum(part_start,
			              + sizeof(part_size)
			              + sizeof(part_raw_size)
			              + part_size,
			              &act_xsum))
				goto ERR1;

			if (exp_xsum != act_xsum) {
				__ERROR("partition[%d], xsum check error ,"
				        "exp_xsum [%" PRIu32 "], act_xsum, [%" PRIu32 "] ,"
				        "size: [%" PRIu32 "], raw_size:[%" PRIu32 "]",
				        i,
				        exp_xsum, act_xsum,
				        part_size, part_raw_size);
				r = NESS_PART_XSUM_ERR;
				goto ERR1;
			}

			if (part_size > 0) {
				struct buffer *part_rbuf;

				part_rbuf = buf_new(part_raw_size);
				ness_decompress(part_datas,
				                part_size,
				                part_rbuf->buf,
				                part_raw_size);
				r = _buf_to_msgbuf(part_rbuf,
				                   part_raw_size,
				                   node->u.n.parts[i].buffer);
				buf_free(part_rbuf);

				if (r != NESS_OK) {
					r = NESS_BUF_TO_MSGBUF_ERR;
					goto ERR;
				}
				node->u.n.parts[i].fetched = 1;
			}
		}
	}

	*n = node;
	buf_free(pivots_rbuf);

	return NESS_OK;

ERR:
	xfree(node);

	return r;

ERR1:
	buf_free(pivots_rbuf);
	xfree(node);

	return r;

}

int _deserialize_nonleaf_from_disk(int fd,
                                   struct block_pair *bp,
                                   struct node **node,
                                   int light)
{
	int r = NESS_ERR;
	uint32_t read_size;
	uint32_t real_size;
	struct buffer *rbuf = NULL;

	real_size = (light == 1) ? (bp->skeleton_size) : (bp->real_size);
	read_size = ALIGN(real_size);
	rbuf = buf_new(read_size);
	if (ness_os_pread(fd,
	                  rbuf->buf,
	                  read_size,
	                  bp->offset) != (ssize_t)read_size) {
		r = NESS_READ_ERR;
		goto ERR;
	}

	/*
	 * check the checksum
	 */
	if (!buf_skip(rbuf, real_size - CRC_SIZE)) goto ERR;

	uint32_t exp_xsum;
	uint32_t act_xsum;

	if (!buf_getuint32(rbuf, &exp_xsum)) goto ERR;
	if (!buf_xsum(rbuf->buf, real_size - CRC_SIZE, &act_xsum)) goto ERR;
	if (exp_xsum != act_xsum) {
		__ERROR("nonleaf xsum check error, "
		        "exp_xsum [%" PRIu32 "], act_xsum [%" PRIu32 "], "
		        "read size [%" PRIu32 "], nid [%" PRIu64 "]",
		        exp_xsum, act_xsum,
		        real_size, bp->nid);
		r = NESS_INNER_XSUM_ERR;
		goto ERR;
	}

	r = _deserialize_nonleaf_from_buf(rbuf, bp, node, light);
	buf_free(rbuf);

	return r;

ERR:
	buf_free(rbuf);

	return r;
}

int serialize_node_to_disk(int fd,
                           struct block *block,
                           struct node *node,
                           struct hdr *hdr)
{
	int r;
	DISKOFF address;
	struct buffer *wbuf;
	uint32_t real_size;
	uint32_t skeleton_size = 0;

	wbuf = buf_new(1 << 20);
	if (node->height == 0)
		_serialize_leaf_to_buf(wbuf, node, hdr);
	else
		_serialize_nonleaf_to_buf(wbuf, node, hdr, &skeleton_size);

	real_size = wbuf->NUL;
	address = block_alloc_off(block,
	                          node->nid,
	                          real_size,
	                          skeleton_size,
	                          node->height);

	buf_putnull(wbuf, ALIGN(real_size) - real_size);
	if (ness_os_pwrite(fd,
	                   wbuf->buf,
	                   ALIGN(real_size),
	                   address) != 0) {
		r = NESS_WRITE_ERR;
		__ERROR("--write node to disk error, fd %d, "
		        "align size [%" PRIu32 "]",
		        fd,
		        ALIGN(real_size));
		goto ERR;
	}

	buf_free(wbuf);

	return NESS_OK;

ERR:
	buf_free(wbuf);

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

	if (bp->height == 0) {
		r = _deserialize_leaf_from_disk(fd, bp, node);
	} else {

		/*
		 * if light is 1, we just need to read nonleaf info,
		 * others, we need to read the whole innernode from disk
		 */
		r = _deserialize_nonleaf_from_disk(fd, bp, node, light);
	}

	return r;
}

int deserialize_part_from_disk(int fd,
                               struct block *block,
                               NID nid,
                               struct node *node,
                               int idx)
{
	int r;
	DISKOFF read_offset;
	uint32_t try_read_size;
	struct buffer *rbuf = NULL;
	struct block_pair *bp;

	nassert(node->u.n.parts[idx].fetched == 0);
	nassert(idx < (int)node->u.n.n_children);

	r = block_get_off_bynid(block, nid, &bp);
	if (r != NESS_OK)
		return NESS_BLOCK_NULL_ERR;

	/*
	 * first to give a try
	 */
	try_read_size = ALIGN(1024);
	read_offset = bp->offset + node->u.n.parts[idx].inner_offset;

	rbuf = buf_new(try_read_size);
	if (ness_os_pread(fd,
	                  rbuf->buf,
	                  try_read_size,
	                  read_offset) != (ssize_t)try_read_size) {
		__ERROR("read part[%d] data error, errno:%d",
		        idx,
		        r);
		r = NESS_READ_ERR;
		goto ERR;
	}

	char *part_datas;
	uint32_t exp_xsum;
	uint32_t act_xsum;
	uint32_t compress_size;
	uint32_t uncompress_size;

	if (!buf_getuint32(rbuf, &compress_size)) goto ERR;

	/* if try fail, re-read with new size */
	if (compress_size > try_read_size) {
		try_read_size = ALIGN(compress_size);

		buf_free(rbuf);
		rbuf = buf_new(try_read_size);

		if (ness_os_pread(fd,
		                  rbuf->buf,
		                  try_read_size,
		                  read_offset) != (ssize_t)try_read_size) {
			r = NESS_READ_ERR;
			goto ERR;
		}

		if (!buf_getuint32(rbuf, &compress_size)) goto ERR;
	}

	if (!buf_getuint32(rbuf, &uncompress_size)) goto ERR;
	if (!buf_getnstr(rbuf, compress_size, (char**)&part_datas)) goto ERR;
	if (!buf_getuint32(rbuf, &exp_xsum)) goto ERR;
	if (!buf_xsum(rbuf->buf,
	              sizeof(compress_size)
	              + sizeof(uncompress_size)
	              + compress_size,
	              &act_xsum))
		goto ERR;

	if (exp_xsum != act_xsum) {
		__ERROR("partition[%d], xsum check error, "
		        "exp_xsum [%" PRIu32 "], act_xsum, [%" PRIu32 "], "
		        "size:[%" PRIu32 "] , raw_size: [%" PRIu32 "]",
		        idx,
		        exp_xsum, act_xsum,
		        compress_size, uncompress_size);
		r = NESS_PART_XSUM_ERR;
		goto ERR;
	}

	if (compress_size > 0) {
		struct buffer *part_rbuf;

		part_rbuf = buf_new(uncompress_size);
		ness_decompress(part_datas,
		                compress_size,
		                part_rbuf->buf,
		                uncompress_size);
		r = _buf_to_msgbuf(part_rbuf,
		                   uncompress_size,
		                   node->u.n.parts[idx].buffer);
		buf_free(part_rbuf);

		if (r != NESS_OK) {
			__ERROR("buffer to fifo error when load part, idx %d, "
			        "nid [%" PRIu64 "]",
			        idx,
			        nid);
			r = NESS_BUF_TO_MSGBUF_ERR;
			goto ERR;
		}

		node->u.n.parts[idx].fetched = 1;
	}

	buf_free(rbuf);

	return r;

ERR:
	buf_free(rbuf);
	return NESS_ERR;
}
