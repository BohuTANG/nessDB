/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#include "u.h"
#include "t.h"

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

static int deserialize_node_header(struct hdr *hdr,
                                   NID nid,
                                   struct msgpack *packer,
                                   struct node **node)
{
	int r = NESS_OK;
	int height;
	int n_children;
	int layout_version;

	/* 1) magic */
	msgpack_skip(packer, 8);

	/* 2) height */
	msgpack_unpack_uint32(packer, (uint32_t*)&height);

	/* 3) children */
	msgpack_unpack_uint32(packer, (uint32_t*)&n_children);

	/* 4) version */
	msgpack_unpack_uint32(packer, (uint32_t*)&layout_version);

	/* 5) xsum */
	uint32_t exp_xsum = 0;
	uint32_t act_xsum = 0;

	do_xsum(packer->base, packer->SEEK, &act_xsum);
	msgpack_unpack_uint32(packer, &exp_xsum);

	if (exp_xsum != act_xsum) {
		r = NESS_BAD_XSUM_ERR;
		goto ERR;
	}

	if (height == 0)
		leaf_new(hdr, nid, height, n_children, node);
	else
		inter_new(hdr, nid, height, n_children, node);
	(*node)->i->init_msgbuf(*node);

ERR:
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
		struct disk_info *disk_info = &node->parts[i].disk_info;

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
		struct disk_info *disk_info = &node->parts[i].disk_info;

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
		struct disk_info *disk_info = &node->parts[i].disk_info;

		disk_info->compressed_ptr = packer->base + node->skeleton_size + disk_info->start;
	}

	return r;
}

static void serialize_node_partition(struct msgpack *packer, struct node *node, int i)
{
	struct disk_info *disk_info = &node->parts[i].disk_info;

	/* 1) partition compressed datas */
	msgpack_pack_nstr(packer, disk_info->compressed_ptr, disk_info->compressed_size);

	/* 2) xsum */
	msgpack_pack_uint32(packer, disk_info->xsum);
}

static int deserialize_node_partition(struct msgpack *packer, struct node *node, int i)
{
	int r = NESS_OK;
	uint32_t seek = packer->SEEK;
	struct disk_info *disk_info = &node->parts[i].disk_info;

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
	struct partition *part;
	struct msgpack *part_packer = msgpack_new(1 << 20);
	ness_compress_method_t method = hdr->opts->compress_method;

	for (i = 0; i < node->n_children; i++) {
		part = &node->parts[i];
		struct disk_info *disk_info = &part->disk_info;

		msgpack_clear(part_packer);
		node->i->mb_packer(part_packer, part->msgbuf);
		disk_info->start = start;
		disk_info->uncompressed_size = part_packer->NUL;
		bound = ness_compress_bound(method, disk_info->uncompressed_size);
		disk_info->compressed_ptr =  xcalloc(1, bound);

		/* compress */
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
	struct partition *part;
	struct msgpack *part_packer;

	for (i = 0; i < node->n_children; i++) {
		part = &node->parts[i];
		struct disk_info *disk_info = &part->disk_info;

		part_packer = msgpack_new(disk_info->uncompressed_size);
		/* decompress */
		r = ness_decompress(disk_info->compressed_ptr, disk_info->compressed_size,
		                    part_packer->base, disk_info->uncompressed_size);

		/* maybe compressed data is NULL */
		if (r == NESS_OK)
			node->i->mb_unpacker(part_packer, part->msgbuf);
		msgpack_free(part_packer);
	}
}

void serialize_node_end(struct node *node)
{
	int i;

	for (i = 0; i < node->n_children; i++) {
		struct disk_info *disk_info = &node->parts[i].disk_info;

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

void deserialize_node_from_packer(struct hdr *hdr,
                                  NID nid,
                                  struct msgpack *packer,
                                  struct node **node)
{
	struct node *n = NULL;

	/* 1) node header */
	deserialize_node_header(hdr, nid, packer, &n);
	nassert(n);

	/* 2) node info */
	deserialize_node_info(packer, n);

	/* 3) node partitions */
	decompress_partitions(n);
	deserialize_node_partitions(packer, n);

	/* 4) end */
	deserialize_node_end(n);

	*node = n;
}

int serialize_node_to_disk(int fd, struct node *node, struct hdr *hdr)
{
	int r;
	uint32_t xsum = 0;
	uint32_t real_size;
	uint32_t write_size;
	DISKOFF address;
	struct tree_options *opts = hdr->opts;
	struct block *block = hdr->block;
	struct msgpack *write_packer = msgpack_new(1 << 20);

	serialize_node_to_packer(write_packer, node, hdr);
	do_xsum(write_packer->base, write_packer->NUL, &xsum);
	msgpack_pack_uint32(write_packer, xsum);

	real_size = write_packer->NUL;
	write_size = ALIGN(real_size);
	address = block_alloc_off(block, node->nid, real_size, node->skeleton_size, node->height);
	msgpack_pack_null(write_packer, write_size - real_size);

	if (opts->vfs->pwrite(fd, write_packer->base, write_size, address) != 0) {
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
	struct tree_options *opts = hdr->opts;
	struct block *block = hdr->block;
	struct msgpack *read_packer = NULL;

	r = block_get_off_bynid(block, nid, &bp);
	if (r != NESS_OK)
		return NESS_BLOCK_NULL_ERR;

	real_size = bp->real_size;
	read_size = ALIGN(real_size);
	read_packer = msgpack_new(read_size);
	if (opts->vfs->pread(fd, read_packer->base, read_size, bp->offset) != (ssize_t)read_size) {
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
	deserialize_node_from_packer(hdr, nid, read_packer, node);

	msgpack_free(read_packer);

	return r;

ERR:
	msgpack_free(read_packer);

	return r;
}
