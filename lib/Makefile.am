SUBDIRS = alpha db io map time

lib_LTLIBRARIES = libgrnxx.la

libgrnxx_la_LIBADD =		\
	alpha/libgrnxx_alpha.la	\
	db/libgrnxx_db.la	\
	io/libgrnxx_io.la	\
	map/libgrnxx_map.la	\
	time/libgrnxx_time.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =		\
	backtrace.cpp		\
	error.cpp		\
	grnxx.cpp		\
	logger.cpp		\
	map.cpp			\
	mutex.cpp		\
	os.cpp			\
	recycler.cpp		\
	slice.cpp		\
	storage.cpp		\
	string.cpp		\
	string_builder.cpp	\
	thread.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =	\
	backtrace.hpp		\
	basic.hpp		\
	error.hpp		\
	exception.hpp		\
	features.hpp		\
	flags_impl.hpp		\
	grnxx.hpp		\
	intrinsic.hpp		\
	lock.hpp		\
	logger.hpp		\
	map.hpp			\
	mutex.hpp		\
	os.hpp			\
	recycler.hpp		\
	slice.hpp		\
	storage.hpp		\
	string.hpp		\
	string_builder.hpp	\
	string_format.hpp	\
	thread.hpp		\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

grnxx.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
