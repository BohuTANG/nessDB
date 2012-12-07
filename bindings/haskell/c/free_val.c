#include "free_val.h"

void
free_val(struct slice *val_slice)
{
    free(val_slice->data);
}
