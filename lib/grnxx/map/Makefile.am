SUBDIRS =						\
	double_array					\
	patricia

noinst_LTLIBRARIES = libgrnxx_map.la

libgrnxx_map_la_LIBADD =				\
	double_array/libgrnxx_map_double_array.la	\
	patricia/libgrnxx_map_patricia.la

libgrnxx_map_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_map_la_SOURCES =				\
	array_map.cpp					\
	bytes_pool.cpp					\
	common_header.cpp				\
	cursor_impl.cpp					\
	double_array.cpp				\
	hash_table.cpp					\
	key_pool.cpp					\
	patricia.cpp					\
	scanner_impl.cpp

libgrnxx_map_includedir = ${includedir}/grnxx/map
libgrnxx_map_include_HEADERS =				\
	array_map.hpp					\
	bytes_pool.hpp					\
	common_header.hpp				\
	cursor_impl.hpp					\
	double_array.hpp				\
	hash.hpp					\
	hash_table.hpp					\
	header.hpp					\
	helper.hpp					\
	key_pool.hpp					\
	patricia.hpp					\
	scanner_impl.hpp
