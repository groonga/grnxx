#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.67])
AC_INIT([grnxx], [dummy13], [groonga@razil.jp])
AC_CONFIG_SRCDIR([lib/grnxx/library.cpp])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE

# Checks for programs.
LT_INIT([win32-dll])
AC_PROG_CXX([clang++ g++-4.8])
AC_PROG_INSTALL

AC_CONFIG_MACRO_DIR([m4])

AM_CXXFLAGS="-Wall -Wextra -std=c++11 -fno-strict-aliasing"
AM_CXXFLAGS="${AM_CXXFLAGS} -I\${top_srcdir}/include -I\${top_srcdir}/lib"

AC_C_BIGENDIAN
AC_CANONICAL_HOST

case "${host}" in
  *-*-mingw*|*-*-cygwin*)
#    AM_CXXFLAGS="${AM_CXXFLAGS} -DGRNXX_DLL"
    AM_LTLDFLAGS="-no-undefined"
    ;;
  *)
    AM_LTLDFLAGS=""
    ;;
esac

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

AC_CONFIG_FILES([Makefile
                 include/Makefile
                 include/grnxx/Makefile
                 include/grnxx/data_types/Makefile
                 include/grnxx/data_types/scalar/Makefile
                 include/grnxx/data_types/vector/Makefile
                 include/grnxx/features/Makefile
                 lib/Makefile
                 lib/grnxx/Makefile
                 lib/grnxx/impl/Makefile
                 lib/grnxx/impl/column/Makefile
                 lib/grnxx/impl/column/scalar/Makefile
                 lib/grnxx/impl/column/vector/Makefile
                 src/Makefile
                 test/Makefile
                 benchmark/Makefile])
AC_OUTPUT

echo
echo "$PACKAGE_NAME $PACKAGE_VERSION configuration:"
echo "-----------------------"
echo "  CXX:                   ${CXX}"
echo "  CXXFLAGS:              ${CXXFLAGS} ${AM_CXXFLAGS}"
echo "  LTLDFLAGS:             ${AM_LTLDFLAGS}"
echo "  LIBS:                  ${LIBS}"
echo
echo "Paths:"
echo "  Install path prefix:   ${prefix}"
echo

echo "Now type 'make' to build ${PACKAGE_NAME} ${PACKAGE_VERSION}!"
