#SUBDIRS =					\
#	sub

lib_LTLIBRARIES = libgrnxx_impl_column.la

#libgrnxx_impl_column_la_LIBADD =		\
#	sub/libgrnxx_impl_column_sub.la

libgrnxx_impl_column_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_impl_column_la_SOURCES =		\
	column.cpp				\
	column_base.cpp				\
	column_int.cpp				\
	column_text.cpp				\
	column_vector_int.cpp			\
	column_vector_float.cpp			\
	column_vector_geo_point.cpp		\
	column_vector_text.cpp

#	column_bool.cpp				\
#	column_float.cpp			\
#	column_geo_point.cpp			\
#	column_vector_bool.cpp

libgrnxx_impl_column_includedir = ${includedir}/grnxx/impl/column
libgrnxx_impl_column_include_HEADERS =		\
	column.hpp				\
	column_base.hpp				\
	column_bool.hpp				\
	column_float.hpp			\
	column_geo_point.hpp			\
	column_int.hpp				\
	column_vector_bool.hpp			\
	column_vector_float.hpp			\
	column_vector_geo_point.hpp		\
	column_vector_int.hpp			\
	column_vector_text.hpp
