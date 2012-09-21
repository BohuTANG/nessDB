#include "../engine/compact.h"
#include "../engine/debug.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

int main()
{
	struct compact *cpt = cpt_new();

	if (!cpt)
		abort();

	cpt_add(cpt, 12, 121);
	cpt_add(cpt, 12, 122);
	cpt_add(cpt, 15, 15);
	cpt_add(cpt, 16, 16);

	__INFO("get val_len#%d, offset:#%lu", 12, cpt_get(cpt, 12));
	__INFO("get val_len#%d, offset:#%lu", 13, cpt_get(cpt, 13));
	__INFO("get val_len#%d, offset:#%lu", 12, cpt_get(cpt, 12));
	__INFO("get val_len#%d, offset:#%lu", 12, cpt_get(cpt, 12));
	__INFO("get val_len#%d, offset:#%lu", 12, cpt_get(cpt, 12));

	cpt_free(cpt);

	return 1;
}
