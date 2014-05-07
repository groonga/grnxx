#SUBDIRS =				\
#	sub

lib_LTLIBRARIES = libgrnxx.la

#libgrnxx_la_LIBADD =			\
#	sub/libgrnxx_sub.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =			\
	calc.cpp			\
	calc_impl.cpp			\
	column.cpp			\
	column_impl.cpp			\
	database.cpp			\
	datum.cpp			\
	index.cpp			\
	library.cpp			\
	sorter.cpp			\
	sorter_impl.cpp			\
	string.cpp			\
	table.cpp			\
	timer.cpp			\
	types.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =		\
	calc.hpp			\
	calc_impl.hpp			\
	column.hpp			\
	column_impl.hpp			\
	database.hpp			\
	datum.hpp			\
	index.hpp			\
	library.hpp			\
	sorter.hpp			\
	sorter_impl.hpp			\
	string.hpp			\
	table.hpp			\
	types.hpp			\
	timer.hpp			\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

library.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
