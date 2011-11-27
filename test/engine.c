#include <check.h>
#include <stdlib.h>

extern void tests_llru(Suite *);

static Suite *
engine_suite(void) {
	Suite *s = suite_create("nessDB Engine");
	tests_llru(s);
	return s;
}

int
main (void) {
  int failed;
  SRunner *sr = srunner_create( engine_suite() );

  srunner_run_all(sr, CK_NORMAL);
  failed = srunner_ntests_failed(sr);
  srunner_free(sr);
  sr = NULL;

  return (failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
