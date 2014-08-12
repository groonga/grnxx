#SUBDIRS =				\
#	sub

lib_LTLIBRARIES = libgrnxx.la

#libgrnxx_la_LIBADD =			\
#	sub/libgrnxx_sub.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =			\
	array.cpp			\
	column.cpp			\
	db.cpp				\
	error.cpp			\
	expression.cpp			\
	index.cpp			\
	library.cpp			\
	name.cpp			\
	sorter.cpp			\
	table.cpp			\
	types.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =		\
	column_impl.hpp			\
	name.hpp			\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

library.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
