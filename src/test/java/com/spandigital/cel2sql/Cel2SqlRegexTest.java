package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.mysql.MySqlDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regex matching tests covering simple patterns, word boundary conversion,
 * and character class conversion across PostgreSQL, MySQL, DuckDB, and BigQuery.
 * SQLite is skipped as it does not support regex.
 * Mirrors the test cases from Go's testcases/regex_tests.go.
 */
class Cel2SqlRegexTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    static Stream<Arguments> regexTests() {
        return Stream.of(
            // simple_match: regex operator syntax differs per dialect
            Arguments.of("simple_match", "name.matches(\"a+\")", "PostgreSQL", PG,
                "name ~ 'a+'"),
            Arguments.of("simple_match", "name.matches(\"a+\")", "MySQL", MYSQL,
                "name REGEXP 'a+'"),
            Arguments.of("simple_match", "name.matches(\"a+\")", "DuckDB", DUCKDB,
                "name ~ 'a+'"),
            Arguments.of("simple_match", "name.matches(\"a+\")", "BigQuery", BQ,
                "REGEXP_CONTAINS(name, 'a+')"),

            // word_boundary: PG converts \b to \y, others keep \b
            Arguments.of("word_boundary", "name.matches(\"\\\\btest\\\\b\")", "PostgreSQL", PG,
                "name ~ '\\ytest\\y'"),
            Arguments.of("word_boundary", "name.matches(\"\\\\btest\\\\b\")", "MySQL", MYSQL,
                "name REGEXP '\\btest\\b'"),
            Arguments.of("word_boundary", "name.matches(\"\\\\btest\\\\b\")", "DuckDB", DUCKDB,
                "name ~ '\\btest\\b'"),
            Arguments.of("word_boundary", "name.matches(\"\\\\btest\\\\b\")", "BigQuery", BQ,
                "REGEXP_CONTAINS(name, '\\btest\\b')"),

            // digit_class: PG converts \d to [[:digit:]], others keep \d
            Arguments.of("digit_class", "name.matches(\"\\\\d{3}-\\\\d{4}\")", "PostgreSQL", PG,
                "name ~ '[[:digit:]]{3}-[[:digit:]]{4}'"),
            Arguments.of("digit_class", "name.matches(\"\\\\d{3}-\\\\d{4}\")", "MySQL", MYSQL,
                "name REGEXP '\\d{3}-\\d{4}'"),
            Arguments.of("digit_class", "name.matches(\"\\\\d{3}-\\\\d{4}\")", "DuckDB", DUCKDB,
                "name ~ '\\d{3}-\\d{4}'"),
            Arguments.of("digit_class", "name.matches(\"\\\\d{3}-\\\\d{4}\")", "BigQuery", BQ,
                "REGEXP_CONTAINS(name, '\\d{3}-\\d{4}')")
        );
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("regexTests")
    void testRegex(String name, String celExpr, String dialectName, Dialect dialect,
                   String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
