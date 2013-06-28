SUBDIRS = map

noinst_LTLIBRARIES = libgrnxx_alpha.la

libgrnxx_alpha_la_LIBADD =		\
	map/libgrnxx_alpha_map.la

libgrnxx_alpha_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_alpha_la_SOURCES =		\
	double_array.cpp		\
	dummy.cpp			\
	map.cpp

libgrnxx_alpha_includedir = ${includedir}/grnxx/alpha
libgrnxx_alpha_include_HEADERS =	\
	double_array.hpp		\
	dummy.hpp			\
	map.hpp				\
	map_range.hpp
