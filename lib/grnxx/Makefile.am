#SUBDIRS =				\
#	impl

lib_LTLIBRARIES = libgrnxx.la

#libgrnxx_la_LIBADD =			\
#	impl/libgrnxx_impl.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =			\
	error.cpp			\
	library.cpp

#	array.cpp			\
#	cursor.cpp			\
#	db.cpp				\
#	error.cpp			\
#	expression.cpp			\
#	index.cpp			\
#	merger.cpp			\
#	name.cpp			\
#	pipeline.cpp			\
#	sorter.cpp			\
#	types.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =		\
	version.h

#	name.hpp			\
#	tree_index.hpp

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

library.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
