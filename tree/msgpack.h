/*
 * Copyright (c) 2012-2015 The nessDB Project Developers. All rights reserved.
 * Code is licensed with BSD.
 *
 */

#ifndef nessDB_MSGPACK_H_
#define nessDB_MSGPACK_H_

/**
 * a msgpack stream
 */

struct msgpack {
	char *base;
	uint32_t NUL;
	uint32_t SEEK;
	uint32_t len;
};

struct msgpack *msgpack_new(uint32_t size);
void msgpack_free(struct msgpack *pk);

int msgpack_pack_uint8(struct msgpack *pk, uint8_t d);
int msgpack_unpack_uint8(struct msgpack *pk, uint8_t *v);

int msgpack_pack_uint32(struct msgpack *pk, uint32_t d);
int msgpack_unpack_uint32(struct msgpack *pk, uint32_t *v);

int msgpack_pack_uint64(struct msgpack *pk, uint64_t d);
int msgpack_unpack_uint64(struct msgpack *pk, uint64_t *v);

int msgpack_pack_nstr(struct msgpack *pk, const char *d, uint32_t n);
int msgpack_unpack_nstr(struct msgpack *pk, uint32_t n, char **v);

int msgpack_pack_msg(struct msgpack *pk, struct msg *d);
int msgpack_unpack_msg(struct msgpack *pk, struct msg *v);

int msgpack_pack_null(struct msgpack *pk, uint32_t n);
int msgpack_seek(struct msgpack *pk, uint32_t s);
int msgpack_seekfirst(struct msgpack *pk);
int msgpack_skip(struct msgpack *pk, uint32_t n);
int msgpack_clear(struct msgpack *pk);

#endif /* nessDB_MSGPACK_H_ */
