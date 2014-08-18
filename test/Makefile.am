TESTS =					\
	test_db				\
	test_table			\
	test_column			\
	test_expression			\
	test_sorter			\
	benchmark_grnxx

check_PROGRAMS = $(TESTS)

test_db_SOURCES = test_db.cpp
test_db_LDADD = $(top_srcdir)/lib/grnxx/libgrnxx.la

test_table_SOURCES = test_table.cpp
test_table_LDADD = $(top_srcdir)/lib/grnxx/libgrnxx.la

test_column_SOURCES = test_column.cpp
test_column_LDADD = $(top_srcdir)/lib/grnxx/libgrnxx.la

test_expression_SOURCES = test_expression.cpp
test_expression_LDADD = $(top_srcdir)/lib/grnxx/libgrnxx.la

test_sorter_SOURCES = test_sorter.cpp
test_sorter_LDADD = $(top_srcdir)/lib/grnxx/libgrnxx.la

benchmark_grnxx_SOURCES = benchmark_grnxx.cpp
benchmark_grnxx_LDADD = $(top_srcdir)/lib/grnxx/libgrnxx.la
