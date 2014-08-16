/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "posix.h"
#include "file.h"
#include "version.h"
#include "msgpack.h"
#include "crc32.h"
#include "hdr.h"

struct hdr *hdr_new(struct env *e) {
	struct hdr *hdr = xcalloc(1, sizeof(struct hdr));

	hdr->e = e;
	hdr->height = 0U;
	hdr->last_nid = NID_START;
	hdr->block = block_new(e);

	return hdr;
}

void hdr_free(struct hdr *hdr)
{
	block_free(hdr->block);
	xfree(hdr);
}

NID hdr_next_nid(struct hdr *hdr)
{
	atomic64_increment(&hdr->last_nid);
	nassert(hdr->last_nid >= NID_START);

	return hdr->last_nid;
}

MSN hdr_next_msn(struct hdr *hdr)
{
	atomic64_increment(&hdr->last_msn);

	return hdr->last_msn;
}

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
static int _serialize_blockpairs_to_disk(int fd, struct hdr *hdr)
{
	uint32_t i;
	int r = NESS_OK;
	uint32_t used_count = 0;

	DISKOFF address;
	struct msgpack *packer;
	uint32_t xsum;
	struct block *b = hdr->block;

	packer = msgpack_new(1 << 20);
	for (i = 0; i < b->pairs_used; i++) {
		if (!b->pairs[i].used) continue;
		used_count++;
	}

	msgpack_pack_nstr(packer, "blkpairs", 8);
	msgpack_pack_uint32(packer, used_count);
	for (i = 0; i < b->pairs_used; i++) {
		if (!b->pairs[i].used) continue;

		msgpack_pack_uint64(packer, b->pairs[i].nid);
		msgpack_pack_uint64(packer, b->pairs[i].offset);
		msgpack_pack_uint32(packer, b->pairs[i].real_size);
		msgpack_pack_uint32(packer, b->pairs[i].skeleton_size);
		msgpack_pack_uint32(packer, b->pairs[i].height);
	}

	if (!do_xsum(packer->base, packer->NUL, &xsum)) {
		r = NESS_DO_XSUM_ERR;
		goto ERR;
	}
	msgpack_pack_uint32(packer, xsum);

	uint32_t real_size;
	uint32_t align_size;

	real_size = packer->NUL;
	hdr->blocksize = real_size;
	align_size = ALIGN(real_size);
	msgpack_pack_null(packer, align_size - real_size);

	address = block_alloc_off(b, 0, real_size, 0, 0);
	if (ness_os_pwrite(fd, packer->base, align_size, address) != 0) {
		r = NESS_WRITE_ERR;
		goto ERR;
	}
	hdr->blockoff = address;
	msgpack_free(packer);

	return NESS_OK;

ERR:
	msgpack_free(packer);
	return r;
}

static int _deserialize_blockpairs_from_disk(int fd, struct hdr *hdr)
{
	int r = NESS_ERR;
	uint32_t read_size;
	uint32_t align_size;
	struct msgpack *packer;
	struct block_pair *pairs;

	read_size = hdr->blocksize;
	align_size = ALIGN(read_size);

	packer = msgpack_new(align_size);
	if (ness_os_pread(fd, packer->base, align_size, hdr->blockoff) != (ssize_t)align_size) {
		r = NESS_READ_ERR;
		goto ERR;
	}

	if (!msgpack_seek(packer, read_size - CRC_SIZE))
		goto ERR;

	uint32_t exp_xsum, act_xsum;

	if (!msgpack_unpack_uint32(packer, &exp_xsum)) goto ERR;
	if (!do_xsum(packer->base, hdr->blocksize - CRC_SIZE, &act_xsum)) goto ERR;
	if (exp_xsum != act_xsum) {
		__ERROR("blockpairs xsum check error,"
		        "exp_xsum: [%" PRIu32 "],"
		        "act_xsum: [%" PRIu32 "]",
		        exp_xsum, act_xsum);
		goto ERR;
	}

	msgpack_seekfirst(packer);

	/*
	 * skip magic with 8bytes
	 */
	if (!msgpack_skip(packer, 8)) goto ERR;

	uint32_t i;
	uint32_t block_count = 0U;

	if (!msgpack_unpack_uint32(packer, &block_count)) goto ERR;
	pairs = xcalloc(block_count, sizeof(*pairs));
	for (i = 0; i < block_count; i++) {
		if (!msgpack_unpack_uint64(packer, &pairs[i].nid)) goto ERR1;
		if (!msgpack_unpack_uint64(packer, &pairs[i].offset)) goto ERR1;
		if (!msgpack_unpack_uint32(packer, &pairs[i].real_size)) goto ERR1;
		if (!msgpack_unpack_uint32(packer, &pairs[i].skeleton_size)) goto ERR1;
		if (!msgpack_unpack_uint32(packer, &pairs[i].height)) goto ERR1;
		pairs[i].used = 1;
	}

	if (block_count > 0)
		block_init(hdr->block, pairs, block_count);
	xfree(pairs);

	msgpack_free(packer);

	return NESS_OK;

ERR:
	msgpack_free(packer);

	return r;

ERR1:
	msgpack_free(packer);
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
 *  |      last-msn        |
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
	struct msgpack *packer = NULL;

	nassert(hdr->root_nid >= NID_START);

	packer = msgpack_new(1 << 20);
	msgpack_pack_nstr(packer, "nesshdr*", 8);
	msgpack_pack_uint32(packer, LAYOUT_VERSION);
	msgpack_pack_uint64(packer, hdr->last_nid);
	msgpack_pack_uint64(packer, hdr->last_msn);
	msgpack_pack_uint64(packer, hdr->root_nid);
	msgpack_pack_uint32(packer, hdr->blocksize);
	msgpack_pack_uint64(packer, hdr->blockoff);

	uint32_t xsum;

	if (!do_xsum(packer->base, packer->NUL, &xsum)) {
		r = NESS_DO_XSUM_ERR;
		goto ERR;
	}
	msgpack_pack_uint32(packer, xsum);

	real_size = packer->NUL;
	align_size = ALIGN(real_size);
	msgpack_pack_null(packer, align_size - real_size);

	if (ness_os_pwrite(fd, packer->base, align_size, off) != 0) {
		r = NESS_WRITE_ERR;
		goto ERR;
	}

	msgpack_free(packer);

	return NESS_OK;

ERR:
	msgpack_free(packer);

	return r;
}

/*
 * double-write for header
 */
int serialize_hdr_to_disk(int fd, struct hdr *hdr)
{
	int r;

	DISKOFF v0_write_off = 0UL;
	DISKOFF v1_write_off = ALIGN(512);

	/* first to serialize pairs to disk */
	if (_serialize_blockpairs_to_disk(fd, hdr) != NESS_OK) {
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


int read_hdr_from_disk(int fd, struct hdr *hdr, DISKOFF off)
{
	int r = NESS_ERR;
	struct msgpack *packer = NULL;
	uint32_t exp_xsum, act_xsum;
	uint32_t read_size, align_size;

	read_size = (
	                    + 8		/* magic        */
	                    + 8		/* last nid     */
	                    + 8		/* last msn     */
	                    + 8		/* root nid     */
	                    + 4		/* version      */
	                    + 4		/* block size   */
	                    + 8		/* block offset */
	                    + CRC_SIZE);/* checksum     */

	align_size = ALIGN(read_size);
	packer = msgpack_new(align_size);
	if (ness_os_pread(fd, packer->base, align_size, off) !=
	    (ssize_t)align_size) {
		__ERROR("ness pread error, read size [%" PRIu32 "], "
		        "offset [%" PRIu64 "]", align_size, 0UL);
		r = NESS_READ_ERR;
		goto ERR;
	}

	if (!msgpack_seek(packer, read_size - CRC_SIZE)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &exp_xsum)) goto ERR;
	if (!do_xsum(packer->base, read_size - CRC_SIZE, &act_xsum)) goto ERR;
	if (exp_xsum != act_xsum) {
		__ERROR("header xsum check error, "
		        "exp_xsum: [%" PRIu32 "], "
		        "act_xsum: [%" PRIu32 "], ",
		        exp_xsum, act_xsum);
		r = NESS_HDR_XSUM_ERR;
		goto ERR;
	}

	msgpack_seekfirst(packer);
	if (!msgpack_skip(packer, 8)) goto ERR;
	if (!msgpack_unpack_uint32(packer, (uint32_t*)&hdr->version)) goto ERR;
	if (!msgpack_unpack_uint64(packer, &hdr->last_nid)) goto ERR;
	if (!msgpack_unpack_uint64(packer, &hdr->last_msn)) goto ERR;
	if (!msgpack_unpack_uint64(packer, &hdr->root_nid)) goto ERR;
	if (!msgpack_unpack_uint32(packer, &hdr->blocksize)) goto ERR;
	if (!msgpack_unpack_uint64(packer, &hdr->blockoff)) goto ERR;

	nassert(hdr->root_nid >= NID_START);

	if (hdr->version < LAYOUT_MIN_SUPPORTED_VERSION) {
		r = NESS_LAYOUT_VERSION_OLDER_ERR;
		__ERROR("tree layout too older [%d], "
		        "min_support_version [%d]",
		        hdr->version, LAYOUT_MIN_SUPPORTED_VERSION);
		goto ERR;
	}

	/* block pairs */
	r = _deserialize_blockpairs_from_disk(fd, hdr);
	if (r != NESS_OK) {
		r = NESS_DESERIAL_BLOCKPAIR_ERR;
		goto ERR;
	}

	msgpack_free(packer);
	return NESS_OK;
ERR:
	msgpack_free(packer);
	return r;
}

int deserialize_hdr_from_disk(int fd, struct hdr *h)
{
	int r;
	DISKOFF v0_read_off = 0UL;
	DISKOFF v1_read_off = ALIGN(512);

	r = read_hdr_from_disk(fd, h, v0_read_off);
	if (r != NESS_OK) {
		__ERROR("1st header broken, "
		        "try to read next, nxt-off[%"PRIu64"]", v1_read_off);
		r = read_hdr_from_disk(fd, h, v1_read_off);
	}

	return r;
}
