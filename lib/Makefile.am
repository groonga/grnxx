SUBDIRS = alpha db io

lib_LTLIBRARIES = libgrnxx.la

libgrnxx_la_LIBADD =		\
	alpha/libgrnxx_alpha.la	\
	db/libgrnxx_db.la	\
	io/libgrnxx_io.la

libgrnxx_la_SOURCES =		\
	backtrace.cpp		\
	duration.cpp		\
	error.cpp		\
	grnxx.cpp		\
	logger.cpp		\
	mutex.cpp		\
	os.cpp			\
	recycler.cpp		\
	string.cpp		\
	string_builder.cpp	\
	thread.cpp		\
	time.cpp

libgrnxx_includedir = ${includedir}/grnxx
libgrnxx_include_HEADERS =	\
	backtrace.hpp		\
	basic.hpp		\
	duration.hpp		\
	error.hpp		\
	exception.hpp		\
	features.hpp		\
	flags_impl.hpp		\
	grnxx.hpp		\
	intrinsic.hpp		\
	lock.hpp		\
	logger.hpp		\
	mutex.hpp		\
	os.hpp			\
	recycler.hpp		\
	string.hpp		\
	string_builder.hpp	\
	string_format.hpp	\
	thread.hpp		\
	time.hpp		\
	version.h

EXTRA_DIST = version.sh

.PHONY: FORCE

FORCE:

grnxx.cpp: version.h

version.h: FORCE
	TOP_DIR="$(top_srcdir)" "$(srcdir)/version.sh"
