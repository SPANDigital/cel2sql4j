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
 * String function tests covering startsWith, endsWith, contains, and size
 * operations across all 5 SQL dialects.
 * Mirrors the test cases from Go's testcases/string_tests.go.
 */
class Cel2SqlStringTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    private static Stream<Arguments> allDialects(String name, String celExpr, String sql) {
        return Stream.of(
            Arguments.of(name, celExpr, "PostgreSQL", PG, sql),
            Arguments.of(name, celExpr, "MySQL", MYSQL, sql),
            Arguments.of(name, celExpr, "SQLite", SQLITE, sql),
            Arguments.of(name, celExpr, "DuckDB", DUCKDB, sql),
            Arguments.of(name, celExpr, "BigQuery", BQ, sql)
        );
    }

    static Stream<Arguments> stringTests() {
        return Stream.of(
            // startsWith: LIKE escape differs per dialect
            Stream.of(
                Arguments.of("starts_with", "name.startsWith(\"a\")", "PostgreSQL", PG,
                    "name LIKE 'a%' ESCAPE E'\\\\'"),
                Arguments.of("starts_with", "name.startsWith(\"a\")", "MySQL", MYSQL,
                    "name LIKE 'a%' ESCAPE '\\\\'"),
                Arguments.of("starts_with", "name.startsWith(\"a\")", "SQLite", SQLITE,
                    "name LIKE 'a%' ESCAPE '\\'"),
                Arguments.of("starts_with", "name.startsWith(\"a\")", "DuckDB", DUCKDB,
                    "name LIKE 'a%' ESCAPE '\\'"),
                Arguments.of("starts_with", "name.startsWith(\"a\")", "BigQuery", BQ,
                    "name LIKE 'a%'")
            ),
            // endsWith: LIKE escape differs per dialect
            Stream.of(
                Arguments.of("ends_with", "name.endsWith(\"z\")", "PostgreSQL", PG,
                    "name LIKE '%z' ESCAPE E'\\\\'"),
                Arguments.of("ends_with", "name.endsWith(\"z\")", "MySQL", MYSQL,
                    "name LIKE '%z' ESCAPE '\\\\'"),
                Arguments.of("ends_with", "name.endsWith(\"z\")", "SQLite", SQLITE,
                    "name LIKE '%z' ESCAPE '\\'"),
                Arguments.of("ends_with", "name.endsWith(\"z\")", "DuckDB", DUCKDB,
                    "name LIKE '%z' ESCAPE '\\'"),
                Arguments.of("ends_with", "name.endsWith(\"z\")", "BigQuery", BQ,
                    "name LIKE '%z'")
            ),
            // contains: function name/syntax differs per dialect
            Stream.of(
                Arguments.of("contains", "name.contains(\"abc\")", "PostgreSQL", PG,
                    "POSITION('abc' IN name) > 0"),
                Arguments.of("contains", "name.contains(\"abc\")", "MySQL", MYSQL,
                    "LOCATE('abc', name) > 0"),
                Arguments.of("contains", "name.contains(\"abc\")", "SQLite", SQLITE,
                    "INSTR(name, 'abc') > 0"),
                Arguments.of("contains", "name.contains(\"abc\")", "DuckDB", DUCKDB,
                    "CONTAINS(name, 'abc')"),
                Arguments.of("contains", "name.contains(\"abc\")", "BigQuery", BQ,
                    "STRPOS(name, 'abc') > 0")
            ),
            // size/LENGTH: same across all dialects
            allDialects("size_string", "size(\"test\")", "LENGTH('test')"),
            allDialects("size_string_var", "name.size()", "LENGTH(name)"),
            allDialects("size_string_global", "size(name)", "LENGTH(name)")
        ).flatMap(s -> s);
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("stringTests")
    void testString(String name, String celExpr, String dialectName, Dialect dialect,
                    String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
