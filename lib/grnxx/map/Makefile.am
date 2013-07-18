SUBDIRS =						\
	array_map					\
	double_array					\
	hash_table					\
	patricia

noinst_LTLIBRARIES = libgrnxx_map.la

libgrnxx_map_la_LIBADD =				\
	array_map/libgrnxx_map_array_map.la		\
	double_array/libgrnxx_map_double_array.la	\
	hash_table/libgrnxx_map_hash_table.la		\
	patricia/libgrnxx_map_patricia.la

libgrnxx_map_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_map_la_SOURCES =				\
	array_map.cpp					\
	bytes_array.cpp					\
	bytes_pool.cpp					\
	bytes_store.cpp					\
	cursor_impl.cpp					\
	double_array.cpp				\
	hash_table.cpp					\
	key_store.cpp					\
	patricia.cpp					\
	scanner_impl.cpp

libgrnxx_map_includedir = ${includedir}/grnxx/map
libgrnxx_map_include_HEADERS =				\
	array_map.hpp					\
	bytes_array.hpp					\
	bytes_pool.hpp					\
	bytes_store.hpp					\
	cursor_impl.hpp					\
	double_array.hpp				\
	hash_table.hpp					\
	header.hpp					\
	helper.hpp					\
	key_store.hpp					\
	patricia.hpp					\
	scanner_impl.hpp
