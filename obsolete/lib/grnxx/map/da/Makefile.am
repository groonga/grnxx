SUBDIRS = basic large

noinst_LTLIBRARIES = libgrnxx_map_da.la

libgrnxx_map_da_la_LIBADD =		\
	basic/libgrnxx_map_da_basic.la	\
	large/libgrnxx_map_da_large.la

libgrnxx_map_da_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_map_da_la_SOURCES =		\
	trie.cpp

libgrnxx_map_da_includedir = ${includedir}/grnxx/map/da
libgrnxx_map_da_include_HEADERS =	\
	trie.hpp
