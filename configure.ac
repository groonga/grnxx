#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([grnxx], [pre-alpha], [groonga@razil.jp])
AC_CONFIG_SRCDIR([lib/grnxx.hpp])
AC_CONFIG_HEADERS(config.h)
AM_INIT_AUTOMAKE

# Checks for programs.
LT_INIT([win32-dll])
AC_PROG_CXX
AC_PROG_INSTALL

AC_CONFIG_MACRO_DIR([m4])

AM_CXXFLAGS="-Wall -Wextra -std=c++0x -fno-strict-aliasing"
AM_LTLDFLAGS="-no-undefined"

AC_SUBST([AM_CXXFLAGS])
AC_SUBST([AM_LTLDFLAGS])

# Checks for libraries.

AC_ARG_ENABLE([backtrace],
              [AS_HELP_STRING([--enable-backtrace],
                              [show backtrace on error [default=no]])],
              [],
              [enable_backtrace="no"])
if test "x$enable_backtrace" != "xno"; then
  AC_CHECK_FUNC([backtrace],
                [AC_DEFINE([HAVE_BACKTRACE], [1],
                           [Define to 1 if you have the `backtrace' function])])
  AC_CHECK_LIB([bfd], [bfd_openr])
fi

AC_CHECK_LIB([rt], [clock_gettime])

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

AC_CONFIG_FILES([Makefile
                 lib/Makefile
                 lib/alpha/Makefile
                 lib/db/Makefile
                 lib/io/Makefile
                 lib/map/Makefile
                 lib/map/da/Makefile
                 src/Makefile
                 test/Makefile])
AC_OUTPUT

echo
echo "$PACKAGE_NAME $PACKAGE_VERSION configuration:"
echo "-----------------------"
echo "  CXX:                   ${CXX}"
echo "  CXXFLAGS:              ${CXXFLAGS}"
echo "  Libraries:             ${LIBS}"
echo
echo "Paths:"
echo "  Install path prefix:   ${prefix}"
echo

echo "Now type 'make' to build $PACKAGE_NAME $PACKAGE_VERSION!"
