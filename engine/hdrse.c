/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "buf.h"
#include "file.h"
#include "version.h"
#include "hdrse.h"
#include "compress/compress.h"


/**********************************************************************
 *
 *  tree header seriliaze
 *
 **********************************************************************/

/*
 *
 *  blockpairs store in disk as:
 *  +------------------------------------------------------+
 *  |                    'blkpairs'                        |
 *  +----------+------+-----------+---------------+--------+
 *  |   nid   |  off  | real-size | skeleton-size | height |
 *  +----------+------+-----------+---------------+--------+
 *               ...
 */
int _serialize_blockpairs_to_disk(int fd, struct block *b, struct hdr *hdr)
{
	uint32_t i;
	int r = NESS_OK;
	uint32_t used_count = 0;

	DISKOFF address;
	struct buffer *wbuf;
	uint32_t xsum;

	wbuf = buf_new(1 << 20);
	for (i = 0; i < b->pairs_used; i++) {
		if (!b->pairs[i].used) continue;
		used_count++;
	}

	buf_putnstr(wbuf, "blkpairs", 8);
	buf_putuint32(wbuf, used_count);
	for (i = 0; i < b->pairs_used; i++) {
		if (!b->pairs[i].used) continue;

		buf_putuint64(wbuf, b->pairs[i].nid);
		buf_putuint64(wbuf, b->pairs[i].offset);
		buf_putuint32(wbuf, b->pairs[i].real_size);
		buf_putuint32(wbuf, b->pairs[i].skeleton_size);
		buf_putuint32(wbuf, b->pairs[i].height);
	}

	if (!buf_xsum(wbuf->buf, wbuf->NUL, &xsum)) {
		r = NESS_DO_XSUM_ERR;
		goto ERR;
	}
	buf_putuint32(wbuf, xsum);

	uint32_t real_size;
	uint32_t align_size;

	real_size = wbuf->NUL;
	hdr->blocksize = real_size;
	align_size = ALIGN(real_size);
	buf_putnull(wbuf, align_size - real_size);

	address = block_alloc_off(b, 0, real_size, 0, 0);
	if (ness_os_pwrite(fd,
	                   wbuf->buf,
	                   align_size,
	                   address) != 0) {
		r = NESS_WRITE_ERR;
		goto ERR;
	}
	hdr->blockoff = address;
	buf_free(wbuf);

	return NESS_OK;

ERR:
	buf_free(wbuf);
	return r;
}

int _deserialize_blockpairs_from_disk(int fd, struct block *b, struct hdr *hdr)
{
	int r = NESS_ERR;
	uint32_t read_size;
	uint32_t align_size;
	struct buffer *rbuf;
	struct block_pair *pairs;

	read_size = hdr->blocksize;
	align_size = ALIGN(read_size);

	rbuf = buf_new(align_size);
	if (ness_os_pread(fd,
	                  rbuf->buf,
	                  align_size,
	                  hdr->blockoff) != (ssize_t)align_size) {
		r = NESS_READ_ERR;
		goto ERR;
	}

	if (!buf_seek(rbuf, read_size - CRC_SIZE))
		goto ERR;

	uint32_t exp_xsum, act_xsum;

	if (!buf_getuint32(rbuf, &exp_xsum)) goto ERR;
	if (!buf_xsum(rbuf->buf, hdr->blocksize - CRC_SIZE, &act_xsum)) goto ERR;
	if (exp_xsum != act_xsum) {
		__ERROR("blockpairs xsum check error,"
		        "exp_xsum: [%" PRIu32 "],"
		        "act_xsum: [%" PRIu32 "]",
		        exp_xsum,
		        act_xsum);
		goto ERR;
	}

	buf_seekfirst(rbuf);

	/*
	 * skip magic with 8bytes
	 */
	if (!buf_skip(rbuf, 8)) goto ERR;

	uint32_t i;
	uint32_t block_count = 0U;

	if (!buf_getuint32(rbuf, &block_count)) goto ERR;
	pairs = xcalloc(block_count, sizeof(*pairs));
	for (i = 0; i < block_count; i++) {
		if (!buf_getuint64(rbuf, &pairs[i].nid)) goto ERR1;
		if (!buf_getuint64(rbuf, &pairs[i].offset)) goto ERR1;
		if (!buf_getuint32(rbuf, &pairs[i].real_size)) goto ERR1;
		if (!buf_getuint32(rbuf, &pairs[i].skeleton_size)) goto ERR1;
		if (!buf_getuint32(rbuf, &pairs[i].height)) goto ERR1;
		pairs[i].used = 1;
	}

	if (block_count > 0)
		block_init(b, pairs, block_count);
	xfree(pairs);

	buf_free(rbuf);

	return NESS_OK;

ERR:
	buf_free(rbuf);

	return r;

ERR1:
	buf_free(rbuf);
	xfree(pairs);

	return r;
}

/*
 *  header laytout stored in disk as:
 *  +----------------------+
 *  |     'nesshdr*'       |
 *  +----------------------+
 *  |      version         |
 *  +----------------------+
 *  |      last-nid        |
 *  +----------------------+
 *  |      roo-nid         |
 *  +----------------------+
 *  |      blocksize       |
 *  +----------------------+
 *  |      blockoff        |
 *  +----------------------+
 */
int write_hdr_to_disk(int fd, struct hdr *hdr, DISKOFF off)
{
	int r;
	uint32_t real_size;
	uint32_t align_size;
	struct buffer *wbuf = NULL;

	nassert(hdr->root_nid >= NID_START);

	wbuf = buf_new(1 << 20);
	buf_putnstr(wbuf, "nesshdr*", 8);
	buf_putuint32(wbuf, LAYOUT_VERSION);
	buf_putuint64(wbuf, hdr->last_nid);
	buf_putuint64(wbuf, hdr->last_msn);
	buf_putuint64(wbuf, hdr->root_nid);
	buf_putuint32(wbuf, hdr->blocksize);
	buf_putuint64(wbuf, hdr->blockoff);

	uint32_t xsum;

	if (!buf_xsum(wbuf->buf, wbuf->NUL, &xsum)) {
		r = NESS_DO_XSUM_ERR;
		goto ERR;
	}
	buf_putuint32(wbuf, xsum);

	real_size = wbuf->NUL;
	align_size = ALIGN(real_size);
	buf_putnull(wbuf, align_size - real_size);

	if (ness_os_pwrite(fd,
	                   wbuf->buf,
	                   align_size,
	                   off) != 0) {
		r = NESS_WRITE_ERR;
		goto ERR;
	}

	buf_free(wbuf);
	return NESS_OK;

ERR:
	buf_free(wbuf);
	return r;
}

/*
 * double-write for header
 */
int serialize_hdr_to_disk(int fd, struct block *b, struct hdr *hdr)
{
	int r;

	DISKOFF v0_write_off = 0UL;
	DISKOFF v1_write_off = ALIGN(512);

	/* first to serialize pairs to disk */
	if (_serialize_blockpairs_to_disk(fd, b, hdr) != NESS_OK) {
		r = NESS_SERIAL_BLOCKPAIR_ERR;
		goto ERR;
	}

	r = write_hdr_to_disk(fd, hdr, v0_write_off);
	if (r != NESS_OK)
		goto ERR;

	r = write_hdr_to_disk(fd, hdr, v1_write_off);

ERR:
	return r;
}


int read_hdr_from_disk(int fd, struct block *b, struct hdr **h, DISKOFF off)
{
	int r = NESS_ERR;
	struct hdr *hdr = NULL;
	struct buffer *rbuf = NULL;
	uint32_t exp_xsum, act_xsum;
	uint32_t read_size, align_size;

	hdr = xcalloc(1, sizeof(*hdr));
	read_size = (
	                    + 8		/* magic        */
	                    + 8		/* last nid     */
	                    + 8		/* last msn     */
	                    + 8		/* root nid     */
	                    + 4		/* version      */
	                    + 4		/* block size   */
	                    + 8		/* block offset */
	                    + CRC_SIZE);	/* checksum     */

	align_size = ALIGN(read_size);
	rbuf = buf_new(align_size);
	if (ness_os_pread(fd, rbuf->buf, align_size, off) !=
	    (ssize_t)align_size) {
		__ERROR("ness pread error, read size [%" PRIu32 "], "
		        "offset [%" PRIu64 "]",
		        align_size,
		        0UL);
		r = NESS_READ_ERR;
		goto ERR;
	}

	if (!buf_seek(rbuf, read_size - CRC_SIZE)) goto ERR;
	if (!buf_getuint32(rbuf, &exp_xsum)) goto ERR;
	if (!buf_xsum(rbuf->buf, read_size - CRC_SIZE, &act_xsum)) goto ERR;
	if (exp_xsum != act_xsum) {
		__ERROR("header xsum check error, "
		        "exp_xsum: [%" PRIu32 "], "
		        "act_xsum: [%" PRIu32 "], ",
		        exp_xsum,
		        act_xsum);
		r = NESS_HDR_XSUM_ERR;
		goto ERR;
	}

	buf_seekfirst(rbuf);
	if (!buf_skip(rbuf, 8)) goto ERR;
	if (!buf_getuint32(rbuf, &hdr->version)) goto ERR;
	if (!buf_getuint64(rbuf, &hdr->last_nid)) goto ERR;
	if (!buf_getuint64(rbuf, &hdr->last_msn)) goto ERR;
	if (!buf_getuint64(rbuf, &hdr->root_nid)) goto ERR;
	if (!buf_getuint32(rbuf, &hdr->blocksize)) goto ERR;
	if (!buf_getuint64(rbuf, &hdr->blockoff)) goto ERR;

	nassert(hdr->root_nid >= NID_START);

	if (hdr->version < LAYOUT_MIN_SUPPORTED_VERSION) {
		r = NESS_LAYOUT_VERSION_OLDER_ERR;
		__ERROR("tree layout too older [%d], "
		        "min_support_version [%d]",
		        hdr->version,
		        LAYOUT_MIN_SUPPORTED_VERSION);
		goto ERR;
	}

	/* block pairs */
	r = _deserialize_blockpairs_from_disk(fd, b, hdr);
	if (r != NESS_OK) {
		r = NESS_DESERIAL_BLOCKPAIR_ERR;
		goto ERR;
	}

	*h = hdr;

	buf_free(rbuf);
	return NESS_OK;
ERR:
	buf_free(rbuf);
	xfree(hdr);
	return r;
}

int deserialize_hdr_from_disk(int fd, struct block *b, struct hdr **h)
{
	int r;
	DISKOFF v0_read_off = 0UL;
	DISKOFF v1_read_off = ALIGN(512);

	r = read_hdr_from_disk(fd, b, h, v0_read_off);
	if (r != NESS_OK) {
		__ERROR("1st header broken, "
		        "try to read next, nxt-off[%"PRIu64"]",
		        v1_read_off);
		r = read_hdr_from_disk(fd, b, h, v1_read_off);
	}

	return r;
}
