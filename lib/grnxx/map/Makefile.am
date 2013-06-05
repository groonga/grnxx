SUBDIRS =					\
	array_map				\
	hash_table

noinst_LTLIBRARIES = libgrnxx_map.la

libgrnxx_map_la_LIBADD =			\
	array_map/libgrnxx_map_array_map.la	\
	hash_table/libgrnxx_map_hash_table.la

libgrnxx_map_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_map_la_SOURCES =			\
	array_map.cpp				\
	bytes_array.cpp				\
	bytes_store.cpp				\
	cursor_impl.cpp				\
	double_array.cpp			\
	hash_table.cpp				\
	patricia.cpp				\
	scanner_impl.cpp

libgrnxx_map_includedir = ${includedir}/grnxx/map
libgrnxx_map_include_HEADERS =			\
	array_map.hpp				\
	bytes_array.hpp				\
	bytes_store.hpp				\
	cursor_impl.hpp				\
	double_array.hpp			\
	hash_table.hpp				\
	header.hpp				\
	helper.hpp				\
	patricia.hpp				\
	scanner_impl.hpp
