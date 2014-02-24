#SUBDIRS =				\
#	sub

lib_LTLIBRARIES = libgrnxx.la

#libgrnxx_la_LIBADD =			\
#	sub/libgrnxx_sub.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =			\
	library.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =		\
	library.hpp			\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

grnxx.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
