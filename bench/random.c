#include "random.h"

void _random_buffer(char *key, int length)
{
	int i;
	char salt[36] = "abcdefghijklmnopqrstuvwxyz123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

struct random *rnd_new() {
	struct random *rnd = xcalloc(1, sizeof(*rnd));

	rnd->seed = 301;
	rnd->size = 1048576;
	rnd->buffer = xcalloc(rnd->size, sizeof(char));
	_random_buffer(rnd->buffer, rnd->size);

	return rnd;
}

uint32_t rnd_next(struct random *rnd)
{
	static const uint32_t M = 2147483647L;   // 2^31-1
	static const uint64_t A = 16807;  // bits 14, 8, 7, 5, 2, 1, 0
	uint64_t product = rnd->seed * A;

	rnd->seed = (uint32_t)((product >> 31) + (product & M));
	if (rnd->seed > M) {
		rnd->seed -= M;
	}
	return rnd->seed;
}

char *rnd_str(struct random *rnd, int len)
{
	if ((rnd->pos + len) > rnd->size)
		rnd->pos = 0;

	rnd->pos += len;
	return (rnd->buffer + (rnd->pos - len));
}

void rnd_free(struct random *rnd)
{
	xfree(rnd->buffer);
	xfree(rnd);
}
