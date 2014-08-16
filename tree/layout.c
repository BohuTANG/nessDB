/*
 * Copyright (c) 2012-2014 The nessDB Project Developers. All rights reserved.
 * Code is licensed with GPL. See COPYING.GPL file.
 *
 */

#include "file.h"
#include "mb.h"
#include "nmb.h"
#include "lmb.h"
#include "crc32.h"
#include "msgpack.h"
#include "compress/compress.h"
#include "layout.h"

/*
 * node header
 * layout:
 *	- | magic | height | children | layout version | xsum |
 */
static void serialize_node_header(struct msgpack *packer, struct node *node)
{
	uint32_t xsum = 0;
	struct msgpack *header = msgpack_new(64 << 10);

	/* 1) magic */
	if (node->height == 0)
		msgpack_pack_nstr(header, "nessleaf", 8);
	else
		msgpack_pack_nstr(header, "nessnode", 8);

	/* 2) height */
	msgpack_pack_uint32(header, node->height);

	/* 3) children */
	msgpack_pack_uint32(header, node->n_children);

	/* 4) version */
	msgpack_pack_uint32(header, node->layout_version);

	/* 5) xsum */
	do_xsum(header->base, header->NUL, &xsum);
	msgpack_pack_uint32(header, xsum);

	msgpack_pack_nstr(packer, header->base, header->NUL);
	msgpack_free(header);
}

static int deserialize_node_header(struct msgpack *packer, struct node *node)
{
	int r = NESS_OK;

	/* 1) magic */
	msgpack_skip(packer, 8);

	/* 2) height */
	msgpack_unpack_uint32(packer, (uint32_t*)&node->height);

	/* 3) children */
	msgpack_unpack_uint32(packer, (uint32_t*)&node->n_children);

	/* 4) version */
	msgpack_unpack_uint32(packer, (uint32_t*)&node->layout_version);

	/* 5) xsum */
	uint32_t exp_xsum;
	uint32_t act_xsum;

	do_xsum(packer->base, packer->SEEK, &act_xsum);
	msgpack_unpack_uint32(packer, &exp_xsum);

	if (exp_xsum != act_xsum)
		r = NESS_BAD_XSUM_ERR;

	/* alloc parts */
	node_init_empty(node, node->n_children, node->layout_version);
	node_ptrs_alloc(node);

	return r;
}

static void serialize_node_info(struct msgpack *packer, struct node *node)
{
	int i;
	uint32_t xsum = 0;
	struct msgpack *info = msgpack_new(1 << 20);

	/* 1) pivots */
	for (i = 0; i < node->n_children - 1; i++)
		msgpack_pack_msg(info, &node->pivots[i]);

	/* 2) partition info */
	for (i = 0; i < node->n_children; i++) {
		struct partition_disk_info *disk_info = &node->parts[i].disk_info;

		msgpack_pack_uint32(info, disk_info->compressed_size);
		msgpack_pack_uint32(info, disk_info->uncompressed_size);
		msgpack_pack_uint32(info, disk_info->start);
		msgpack_pack_uint64(info, node->parts[i].child_nid);
	}

	/* 3) xsum */
	do_xsum(info->base, info->NUL, &xsum);
	msgpack_pack_uint32(info, xsum);

	msgpack_pack_nstr(packer, info->base, info->NUL);
	msgpack_free(info);

	/* 4) get skeleton size */
	node->skeleton_size = packer->NUL;
}

static int deserialize_node_info(struct msgpack *packer, struct node *node)
{
	int i;
	int r = NESS_OK;
	uint32_t seek = packer->SEEK;

	/* 1) pivots */
	for (i = 0; i < node->n_children - 1; i++)
		msgpack_unpack_msg(packer, &node->pivots[i]);

	/* 2) partition info */
	for (i = 0; i < node->n_children; i++) {
		struct partition_disk_info *disk_info = &node->parts[i].disk_info;

		msgpack_unpack_uint32(packer, &disk_info->compressed_size);
		msgpack_unpack_uint32(packer, &disk_info->uncompressed_size);
		msgpack_unpack_uint32(packer, &disk_info->start);
		msgpack_unpack_uint64(packer, &node->parts[i].child_nid);
	}

	/* 3) xsum */
	uint32_t exp_xsum;
	uint32_t act_xsum;

	do_xsum(packer->base + seek, packer->SEEK - seek, &act_xsum);
	msgpack_unpack_uint32(packer, &exp_xsum);

	if (exp_xsum != act_xsum)
		r = NESS_BAD_XSUM_ERR;

	/* 4) get skeleton size */
	/* now, it's time to get skeleton size */
	node->skeleton_size = packer->SEEK;
	for (i = 0; i < node->n_children; i++) {
		struct partition_disk_info *disk_info = &node->parts[i].disk_info;

		disk_info->compressed_ptr = packer->base + node->skeleton_size + disk_info->start;
	}

	return r;
}

static void serialize_node_partition(struct msgpack *packer, struct node *node, int i)
{
	struct partition_disk_info *disk_info = &node->parts[i].disk_info;

	/* 1) partition compressed datas */
	msgpack_pack_nstr(packer, disk_info->compressed_ptr, disk_info->compressed_size);

	/* 2) xsum */
	msgpack_pack_uint32(packer, disk_info->xsum);
}

static int deserialize_node_partition(struct msgpack *packer, struct node *node, int i)
{
	int r = NESS_OK;
	uint32_t seek = packer->SEEK;
	struct partition_disk_info *disk_info = &node->parts[i].disk_info;

	/* 1) partition compressed datas */
	msgpack_unpack_nstr(packer, disk_info->compressed_size, &disk_info->compressed_ptr);

	/* 2) xsum */
	uint32_t exp_xsum;
	uint32_t act_xsum = 0U;

	do_xsum(packer->base + seek, packer->SEEK - seek, &act_xsum);
	msgpack_unpack_uint32(packer, &exp_xsum);

	if (exp_xsum != act_xsum)
		r = NESS_BAD_XSUM_ERR;

	return r;
}

static void serialize_node_partitions(struct msgpack *packer, struct node *node)
{
	int i;

	/* first to get skeleton size */
	node->skeleton_size = packer->NUL;

	for (i = 0; i < node->n_children; i++)
		serialize_node_partition(packer, node, i);
}

static int deserialize_node_partitions(struct msgpack *packer, struct node *node)
{
	int i;
	int r = NESS_OK;

	for (i = 0; i < node->n_children; i++) {
		r = deserialize_node_partition(packer, node, i);
		if (r != NESS_OK)
			goto ERR;
	}

ERR:
	return r;
}

/*
 * TODO(BohuTANG):
 *	- if the node is leaf, we should rebalance the buffers under the options->leaf_default_basement_size
 */
void compress_partitions(struct node *node, struct hdr *hdr)
{
	int i;
	uint32_t bound;
	uint32_t start = 0U;
	struct msgpack *part_packer = msgpack_new(1 << 20);
	ness_compress_method_t method = hdr->e->compress_method;

	__DEBUG("-node nid %"PRIu64 " , children %d, height %d", node->nid, node->n_children, node->height);
	for (i = 0; i < node->n_children; i++) {
		struct child_pointer *ptr = &node->parts[i].ptr;
		struct partition_disk_info *disk_info = &node->parts[i].disk_info;

		msgpack_clear(part_packer);
		if (node->height > 0)
			nmb_to_msgpack(ptr->u.nonleaf->buffer, part_packer);
		else
			lmb_to_msgpack(ptr->u.leaf->buffer, part_packer);

		disk_info->start = start;
		disk_info->uncompressed_size = part_packer->NUL;
		bound = ness_compress_bound(method, disk_info->uncompressed_size);
		disk_info->compressed_ptr =  xcalloc(1, bound);

		ness_compress(method, part_packer->base, part_packer->NUL,
		              disk_info->compressed_ptr, &disk_info->compressed_size);

		do_xsum(disk_info->compressed_ptr, disk_info->compressed_size, &disk_info->xsum);
		start += (disk_info->compressed_size + sizeof(disk_info->xsum));
	}
	msgpack_free(part_packer);
}

/*
 * EFFECT:
 *	- decompress partition from compressed datas
 * REQUIRE:
 *	- node disk_info has read
 */
void decompress_partitions(struct node *node)
{
	int r;
	int i;
	struct msgpack *part_packer;

	for (i = 0; i < node->n_children; i++) {
		struct partition_disk_info *disk_info = &node->parts[i].disk_info;

		part_packer = msgpack_new(disk_info->uncompressed_size);
		r = ness_decompress(disk_info->compressed_ptr, disk_info->compressed_size,
		                    part_packer->base, disk_info->uncompressed_size);

		/* maybe compressed data is NULL */
		if (r == NESS_OK) {
			if (node->height > 0)
				msgpack_to_nmb(part_packer, node->parts[i].ptr.u.nonleaf->buffer);
			else
				msgpack_to_lmb(part_packer, node->parts[i].ptr.u.leaf->buffer);
		}
		msgpack_free(part_packer);
	}
}

void serialize_node_end(struct node *node)
{
	int i;

	for (i = 0; i < node->n_children; i++) {
		struct partition_disk_info *disk_info = &node->parts[i].disk_info;

		xfree(disk_info->compressed_ptr);
		disk_info->compressed_ptr = NULL;
		disk_info->uncompressed_ptr = NULL;
	}
}

void deserialize_node_end(struct node *node)
{
	(void)node;
}

void serialize_node_to_packer(struct msgpack *packer, struct node *node, struct hdr *hdr)
{
	/* 1) compress part to subblock */
	compress_partitions(node, hdr);

	/* 2) node header */
	serialize_node_header(packer, node);

	/* 3) node info */
	serialize_node_info(packer, node);

	/* 4) node partitions */
	serialize_node_partitions(packer, node);

	/* 5) end */
	serialize_node_end(node);
}

void deserialize_node_from_packer(struct msgpack *packer, struct node *node, struct hdr *hdr)
{
	(void)hdr;
	/* 1) node header */
	deserialize_node_header(packer, node);

	/* 2) node info */
	deserialize_node_info(packer, node);

	/* 3) node partitions */
	decompress_partitions(node);
	deserialize_node_partitions(packer, node);

	/* 4) end */
	deserialize_node_end(node);
}

int serialize_node_to_disk(int fd, struct node *node, struct hdr *hdr)
{
	int r;
	uint32_t xsum = 0;
	uint32_t real_size;
	uint32_t write_size;
	DISKOFF address;
	struct block *block = hdr->block;
	struct msgpack *write_packer = msgpack_new(1 << 20);

	serialize_node_to_packer(write_packer, node, hdr);
	do_xsum(write_packer->base, write_packer->NUL, &xsum);
	msgpack_pack_uint32(write_packer, xsum);

	real_size = write_packer->NUL;
	write_size = ALIGN(real_size);
	address = block_alloc_off(block, node->nid, real_size, node->skeleton_size, node->height);
	msgpack_pack_null(write_packer, write_size - real_size);

	if (ness_os_pwrite(fd, write_packer->base, write_size, address) != 0) {
		r = NESS_WRITE_ERR;
		__ERROR("--write node to disk error, fd %d, "
		        "align size [%" PRIu32 "]", fd, write_size);
		goto ERR;
	}
	msgpack_free(write_packer);

	return NESS_OK;

ERR:
	msgpack_free(write_packer);

	return r;
}

int deserialize_node_from_disk(int fd, struct hdr *hdr, NID nid, struct node **node)
{
	int r = NESS_OK;
	struct block_pair *bp;

	uint32_t exp_xsum;
	uint32_t act_xsum;
	uint32_t real_size;
	uint32_t read_size;
	struct node *n;
	struct block *block = hdr->block;
	struct msgpack *read_packer = NULL;

	r = block_get_off_bynid(block, nid, &bp);
	if (r != NESS_OK)
		return NESS_BLOCK_NULL_ERR;

	real_size = bp->real_size;
	read_size = ALIGN(real_size);
	read_packer = msgpack_new(read_size);
	if (ness_os_pread(fd, read_packer->base, read_size, bp->offset) != (ssize_t)read_size) {
		r = NESS_READ_ERR;
		goto ERR;
	}

	/* check the checksum */
	if (!msgpack_skip(read_packer, real_size - CRC_SIZE)) goto ERR;
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

	msgpack_seekfirst(read_packer);
	n = node_alloc_empty(nid, bp->height, hdr->e);
	deserialize_node_from_packer(read_packer, n, hdr);
	*node = n;

	msgpack_free(read_packer);

	return r;

ERR:
	msgpack_free(read_packer);

	return r;
}
