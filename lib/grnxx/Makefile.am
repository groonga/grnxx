SUBDIRS =				\
	alpha				\
	charset				\
	db				\
	io				\
	map				\
	storage

lib_LTLIBRARIES = libgrnxx.la

libgrnxx_la_LIBADD =			\
	alpha/libgrnxx_alpha.la		\
	charset/libgrnxx_charset.la	\
	db/libgrnxx_db.la		\
	io/libgrnxx_io.la		\
	map/libgrnxx_map.la		\
	storage/libgrnxx_storage.la

libgrnxx_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_la_SOURCES =			\
	array.cpp			\
	array_impl.cpp			\
	backtrace.cpp			\
	broken_down_time.cpp		\
	bytes.cpp			\
	charset.cpp			\
	duration.cpp			\
	error.cpp			\
	geo_point.cpp			\
	grnxx.cpp			\
	logger.cpp			\
	map.cpp				\
	map_cursor.cpp			\
	map_scanner.cpp			\
	mutex.cpp			\
	os.cpp				\
	periodic_clock.cpp		\
	recycler.cpp			\
	slice.cpp			\
	stopwatch.cpp			\
	storage.cpp			\
	string.cpp			\
	string_builder.cpp		\
	system_clock.cpp		\
	thread.cpp			\
	time.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =		\
	array.hpp			\
	array_impl.hpp			\
	backtrace.hpp			\
	basic.hpp			\
	broken_down_time.hpp		\
	bytes.hpp			\
	charset.hpp			\
	duration.hpp			\
	error.hpp			\
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
	recycler.hpp			\
	slice.hpp			\
	stopwatch.hpp			\
	storage.hpp			\
	string.hpp			\
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
