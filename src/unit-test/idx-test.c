#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "../idx.h"
#include "ct/ct.h"

void cttest_idx_init()
{
	struct idx _idx;
	idx_init(&_idx,10000);
	assert(0 == 0);
}

void cttest_idx_add()
{
	struct idx _idx;
	idx_init(&_idx,10000);
	idx_set(&_idx,"ab",100UL);
	idx_set(&_idx,"cd",5000000000UL);
	uint64_t  ret=idx_get(&_idx,"cd");
	assert(ret == 5000000000UL);
}

void cttest_idx_get()
{
	struct idx _idx;
	idx_init(&_idx,10000);
	idx_set(&_idx,"ab",100UL);
	idx_set(&_idx,"cd",5000000000UL);
	uint64_t  ret=idx_get(&_idx,"ab");
	assert(ret == 100UL);
}

