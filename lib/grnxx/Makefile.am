SUBDIRS =				\
	charset				\
	map				\
	storage

lib_LTLIBRARIES = libgrnxx.la

libgrnxx_la_LIBADD =			\
	charset/libgrnxx_charset.la	\
	map/libgrnxx_map.la		\
	storage/libgrnxx_storage.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =			\
	array.cpp			\
	array_impl.cpp			\
	array2.cpp			\
	array2_impl.cpp			\
	backtrace.cpp			\
	broken_down_time.cpp		\
	charset.cpp			\
	common_header.cpp		\
	duration.cpp			\
	errno.cpp			\
	geo_point.cpp			\
	grnxx.cpp			\
	logger.cpp			\
	map.cpp				\
	map_cursor.cpp			\
	map_scanner.cpp			\
	mutex.cpp			\
	os.cpp				\
	periodic_clock.cpp		\
	stopwatch.cpp			\
	storage.cpp			\
	string_builder.cpp		\
	system_clock.cpp		\
	thread.cpp			\
	time.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =		\
	array.hpp			\
	array_impl.hpp			\
	array2.hpp			\
	array2_impl.hpp			\
	backtrace.hpp			\
	broken_down_time.hpp		\
	bytes.hpp			\
	charset.hpp			\
	common_header.hpp		\
	duration.hpp			\
	errno.hpp			\
	exception.hpp			\
	features.hpp			\
	flags_impl.hpp			\
	geo_point.hpp			\
	grnxx.hpp			\
	intrinsic.hpp			\
	lock.hpp			\
	logger.hpp			\
	map.hpp				\
	map_cursor.hpp			\
	map_cursor_query.hpp		\
	map_scanner.hpp			\
	mutex.hpp			\
	os.hpp				\
	periodic_clock.hpp		\
	stopwatch.hpp			\
	storage.hpp			\
	string_builder.hpp		\
	string_format.hpp		\
	system_clock.hpp		\
	thread.hpp			\
	time.hpp			\
	traits.hpp			\
	types.hpp			\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

grnxx.cpp: version.h

version.h: FORCE
	TOP_SRCDIR="$(top_srcdir)" "$(srcdir)/version.sh"
