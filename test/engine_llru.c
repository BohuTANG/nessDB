#include <check.h>

#include "../engine/llru.h"

llru_t self;

static void
setup(void) {
	llru_init(&self, 1024 * 1024);
}

static void
teardown(void) {
	llru_free(&self);
}

START_TEST(test_a){
	fail_unless(1 == 0, "TODO: write tests");

}END_TEST

void tests_llru(Suite *s) {
	TCase *x = tcase_create("LLRU");
	tcase_add_checked_fixture(x, setup, teardown);
	tcase_add_test(x, test_a);
	suite_add_tcase(s, x);
}