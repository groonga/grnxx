SUBDIRS =				\
	column

lib_LTLIBRARIES = libgrnxx_impl.la

libgrnxx_impl_la_LIBADD =		\
	column/libgrnxx_impl_column.la

libgrnxx_impl_la_LDFLAGS = @AM_LTLDFLAGS@

libgrnxx_impl_la_SOURCES =		\
	db.cpp				\
	expression.cpp			\
	index.cpp			\
	merger.cpp			\
	pipeline.cpp			\
	sorter.cpp			\
	table.cpp

libgrnxx_impl_includedir = ${includedir}/grnxx/impl
libgrnxx_impl_include_HEADERS =		\
	column.hpp			\
	cursor.hpp			\
	db.hpp				\
	expression.hpp			\
	index.hpp			\
	merger.hpp			\
	pipeline.hpp			\
	sorter.hpp			\
	table.hpp
