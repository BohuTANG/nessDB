check_PROGRAMS = test_different_dirs
test_different_dirs_SOURCES = test_different_dirs.c
test_different_dirs_LDADD = $(srcdir)/../nessDB/libnessDB-1.8.la

TESTS = test_different_dirs

clean-local: clean-local-check
.PHONY: clean-local-check
clean-local-check:
	-rm -rf tmp