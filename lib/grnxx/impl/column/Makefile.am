SUBDIRS =					\
	scalar					\
	vector

lib_LTLIBRARIES = libgrnxx_impl_column.la

libgrnxx_impl_column_la_LIBADD =		\
	scalar/libgrnxx_impl_column_scalar.la	\
	vector/libgrnxx_impl_column_vector.la

libgrnxx_impl_column_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_impl_column_la_SOURCES =		\
	base.cpp

libgrnxx_impl_column_includedir = ${includedir}/grnxx/impl/column
libgrnxx_impl_column_include_HEADERS =		\
	base.hpp				\
	scalar.hpp				\
	vector.hpp
