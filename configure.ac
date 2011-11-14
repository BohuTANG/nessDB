#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(nessDB, 1.8.0, nessdb@googlegroups.com)
AM_INIT_AUTOMAKE([foreign -Wall])
AC_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
LT_INIT
# Checks for programs.
AC_PROG_CC

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h netdb.h netinet/in.h stdint.h stdlib.h string.h sys/socket.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_MODE_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_HEADER_TIME
AC_HEADER_STDBOOL
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_STAT
AC_CHECK_FUNCS([gethostbyname gettimeofday inet_ntoa memmove memset mkdir select socket strchr strerror])

AC_CONFIG_FILES([Makefile nessDB/Makefile server/Makefile bench/Makefile])
AC_OUTPUT
