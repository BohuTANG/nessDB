#include <unistd.h>
#include <stdlib.h>
#include "ctest.h"

#include "engine/internal.h"
#include "engine/buffer.h"
#include "engine/debug.h"



CTEST(buffer_test, get_put_char) {
    struct buffer *buf = buffer_new(1024);
    char c=2;
    buffer_putc(buf, c);
    char value=buffer_getchar(buf);
    CTEST_LOG("the value is %d",value);
    ASSERT_EQUAL(c,value);
}

CTEST(buffer_test, get_put_int) {
    struct buffer *buf = buffer_new(1024);
    const int i=256;
    buffer_putint(buf, i);
    int value=buffer_getint(buf);
    CTEST_LOG("the value is %d",value);
    ASSERT_TRUE(i==value);
}

CTEST(buffer_test, get_put_str) {
    struct buffer *buf = buffer_new(1024);
    const char* i="test";
    buffer_putstr(buf, i);
    char* value=buffer_getnstr(buf,4);
    CTEST_LOG("the value is %s",value);
    ASSERT_STR(i,value);
}

CTEST(buffer_test, get_put_long) {
    struct buffer *buf = buffer_new(1024);
    unsigned long i=256234ul;
    buffer_putlong(buf, i);
    buffer_seekfirst(buf);
    unsigned long value=buffer_getlong(buf);
    CTEST_LOG("the value is %lu",value);
    ASSERT_EQUAL(i,value);
}
