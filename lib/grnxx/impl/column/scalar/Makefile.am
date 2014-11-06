#SUBDIRS =					\
#	sub

lib_LTLIBRARIES = libgrnxx_impl_column_scalar.la

#libgrnxx_impl_column_scalar_la_LIBADD =	\
#	sub/libgrnxx_impl_column_scalar_sub.la

libgrnxx_impl_column_scalar_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_impl_column_scalar_la_SOURCES =	\
	bool.cpp				\
	float.cpp				\
	geo_point.cpp				\
	int.cpp

#	text.cpp

libgrnxx_impl_column_scalar_includedir = ${includedir}/grnxx/impl/column/scalar
libgrnxx_impl_column_scalar_include_HEADERS =	\
	bool.hpp				\
	float.hpp				\
	geo_point.hpp				\
	int.hpp					\
	text.hpp
