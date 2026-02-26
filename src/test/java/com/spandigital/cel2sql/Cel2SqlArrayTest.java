package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.mysql.MySqlDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Array/list tests covering list indexing, size, and the IN operator across
 * supported dialects. MySQL/SQLite are skipped for array indexing and size
 * (no native array support). IN-list is tested across all 5 dialects.
 * Mirrors the test cases from Go's testcases/array_tests.go.
 */
class Cel2SqlArrayTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    static Stream<Arguments> arrayTests() {
        return Stream.of(
            // list_index_literal: array indexing only for PG/DuckDB/BQ
            Arguments.of("list_index_literal", "[1, 2, 3][0] == 1", "PostgreSQL", PG,
                "ARRAY[1, 2, 3][1] = 1"),
            Arguments.of("list_index_literal", "[1, 2, 3][0] == 1", "DuckDB", DUCKDB,
                "[1, 2, 3][1] = 1"),
            Arguments.of("list_index_literal", "[1, 2, 3][0] == 1", "BigQuery", BQ,
                "[1, 2, 3][OFFSET(0)] = 1"),

            // size_list: array size only for PG/DuckDB/BQ
            Arguments.of("size_list", "size(string_list)", "PostgreSQL", PG,
                "COALESCE(ARRAY_LENGTH(string_list, 1), 0)"),
            Arguments.of("size_list", "size(string_list)", "DuckDB", DUCKDB,
                "COALESCE(array_length(string_list), 0)"),
            Arguments.of("size_list", "size(string_list)", "BigQuery", BQ,
                "ARRAY_LENGTH(string_list)"),

            // in_list: all 5 dialects with different containment syntax
            Arguments.of("in_list", "name in [\"a\", \"b\", \"c\"]", "PostgreSQL", PG,
                "name = ANY(ARRAY['a', 'b', 'c'])"),
            Arguments.of("in_list", "name in [\"a\", \"b\", \"c\"]", "MySQL", MYSQL,
                "JSON_CONTAINS(JSON_ARRAY('a', 'b', 'c'), CAST(name AS JSON))"),
            Arguments.of("in_list", "name in [\"a\", \"b\", \"c\"]", "SQLite", SQLITE,
                "name IN (SELECT value FROM json_each(json_array('a', 'b', 'c')))"),
            Arguments.of("in_list", "name in [\"a\", \"b\", \"c\"]", "DuckDB", DUCKDB,
                "name = ANY(['a', 'b', 'c'])"),
            Arguments.of("in_list", "name in [\"a\", \"b\", \"c\"]", "BigQuery", BQ,
                "name IN UNNEST(['a', 'b', 'c'])"),

            // size_list_var_method: array size via .size() method
            Arguments.of("size_list_var_method", "string_list.size()", "PostgreSQL", PG,
                "COALESCE(ARRAY_LENGTH(string_list, 1), 0)"),
            Arguments.of("size_list_var_method", "string_list.size()", "DuckDB", DUCKDB,
                "COALESCE(array_length(string_list), 0)"),
            Arguments.of("size_list_var_method", "string_list.size()", "BigQuery", BQ,
                "ARRAY_LENGTH(string_list)")
        );
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("arrayTests")
    void testArray(String name, String celExpr, String dialectName, Dialect dialect,
                   String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
