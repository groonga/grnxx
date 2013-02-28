SUBDIRS = alpha db io map

lib_LTLIBRARIES = libgrnxx.la

libgrnxx_la_LIBADD =		\
	alpha/libgrnxx_alpha.la	\
	db/libgrnxx_db.la	\
	io/libgrnxx_io.la	\
	map/libgrnxx_map.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =		\
	backtrace.cpp		\
	broken_down_time.cpp	\
	duration.cpp		\
	error.cpp		\
	grnxx.cpp		\
	logger.cpp		\
	map.cpp			\
	mutex.cpp		\
	os.cpp			\
	recycler.cpp		\
	slice.cpp		\
	string.cpp		\
	string_builder.cpp	\
	thread.cpp		\
	time.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =	\
	backtrace.hpp		\
	basic.hpp		\
	broken_down_time.hpp	\
	duration.hpp		\
	error.hpp		\
	exception.hpp		\
	features.hpp		\
	flags_impl.hpp		\
	grnxx.hpp		\
	internal_clock.hpp	\
	intrinsic.hpp		\
	lock.hpp		\
	logger.hpp		\
	map.hpp			\
	mutex.hpp		\
	os.hpp			\
	recycler.hpp		\
	slice.hpp		\
	steady_clock.hpp	\
	string.hpp		\
	string_builder.hpp	\
	string_format.hpp	\
	system_clock.hpp	\
	thread.hpp		\
	time.hpp		\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

grnxx.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
