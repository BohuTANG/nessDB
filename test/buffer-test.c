#include <unistd.h>
#include <stdlib.h>
#include "ctest.h"

#include "engine/internal.h"
#include "engine/buffer.h"
#include "engine/debug.h"


CTEST(buffer_test, get_put_int) {
    struct buffer *buf = buffer_new(1024);
    uint32_t i=255;
    buffer_putc(buf, i);
    buffer_seekfirst(buf);
    uint32_t value=buffer_getchar(buf);
    CTEST_LOG("the value is %u",value);
    ASSERT_EQUAL(i,value);
}


CTEST(buffer_test, get_put_char) {
	struct buffer *buf = buffer_new(1024);
    char c=2;
	buffer_putc(buf, c);
    buffer_seekfirst(buf);
    char value=buffer_getchar(buf);
    CTEST_LOG("the value is %d",value);
    ASSERT_EQUAL(c,value);
}
