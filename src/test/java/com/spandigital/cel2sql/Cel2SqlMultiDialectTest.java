package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.mysql.MySqlDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import com.spandigital.cel2sql.testutil.CelHelper;
import dev.cel.common.CelAbstractSyntaxTree;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Multi-dialect tests that verify CEL expression conversion produces
 * correct SQL output for each supported dialect.
 */
class Cel2SqlMultiDialectTest {

    private static String convert(String celExpr, Dialect dialect) throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        return Cel2Sql.convert(ast, opts);
    }

    // ---- String equality - same across all dialects except BigQuery string literal escaping ----

    static Stream<Arguments> stringEqualityTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "name = 'a'"),
            Arguments.of("MySQL", new MySqlDialect(), "name = 'a'"),
            Arguments.of("SQLite", new SqliteDialect(), "name = 'a'"),
            Arguments.of("DuckDB", new DuckDbDialect(), "name = 'a'"),
            Arguments.of("BigQuery", new BigQueryDialect(), "name = 'a'")
        );
    }

    @ParameterizedTest(name = "{0}: name == \"a\"")
    @MethodSource("stringEqualityTests")
    @DisplayName("String equality across dialects")
    void testStringEquality(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("name == \"a\"", dialect)).isEqualTo(expectedSql);
    }

    // ---- Logical AND + OR ----

    static Stream<Arguments> logicalAndOrTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "name = 'a' AND age > 20"),
            Arguments.of("MySQL", new MySqlDialect(), "name = 'a' AND age > 20"),
            Arguments.of("SQLite", new SqliteDialect(), "name = 'a' AND age > 20"),
            Arguments.of("DuckDB", new DuckDbDialect(), "name = 'a' AND age > 20"),
            Arguments.of("BigQuery", new BigQueryDialect(), "name = 'a' AND age > 20")
        );
    }

    @ParameterizedTest(name = "{0}: name == \"a\" && age > 20")
    @MethodSource("logicalAndOrTests")
    @DisplayName("Logical AND across dialects")
    void testLogicalAnd(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("name == \"a\" && age > 20", dialect)).isEqualTo(expectedSql);
    }

    // ---- String startsWith (LIKE pattern) ----

    static Stream<Arguments> startsWithTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "name LIKE 'admin%' ESCAPE E'\\\\'"),
            Arguments.of("MySQL", new MySqlDialect(), "name LIKE 'admin%' ESCAPE '\\\\'"),
            Arguments.of("SQLite", new SqliteDialect(), "name LIKE 'admin%' ESCAPE '\\\\'"),
            Arguments.of("DuckDB", new DuckDbDialect(), "name LIKE 'admin%' ESCAPE '\\\\'"),
            Arguments.of("BigQuery", new BigQueryDialect(), "name LIKE 'admin%'")
        );
    }

    @ParameterizedTest(name = "{0}: name.startsWith(\"admin\")")
    @MethodSource("startsWithTests")
    @DisplayName("String startsWith across dialects")
    void testStartsWith(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("name.startsWith(\"admin\")", dialect)).isEqualTo(expectedSql);
    }

    // ---- String contains ----

    static Stream<Arguments> containsTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "POSITION('test' IN name) > 0"),
            Arguments.of("MySQL", new MySqlDialect(), "LOCATE('test', name) > 0"),
            Arguments.of("SQLite", new SqliteDialect(), "INSTR(name, 'test') > 0"),
            Arguments.of("DuckDB", new DuckDbDialect(), "CONTAINS(name, 'test')"),
            Arguments.of("BigQuery", new BigQueryDialect(), "STRPOS(name, 'test') > 0")
        );
    }

    @ParameterizedTest(name = "{0}: name.contains(\"test\")")
    @MethodSource("containsTests")
    @DisplayName("String contains across dialects")
    void testContains(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("name.contains(\"test\")", dialect)).isEqualTo(expectedSql);
    }

    // ---- String concatenation ----

    static Stream<Arguments> stringConcatTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "name || 'suffix'"),
            Arguments.of("MySQL", new MySqlDialect(), "CONCAT(name, 'suffix')"),
            Arguments.of("SQLite", new SqliteDialect(), "name || 'suffix'"),
            Arguments.of("DuckDB", new DuckDbDialect(), "name || 'suffix'"),
            Arguments.of("BigQuery", new BigQueryDialect(), "name || 'suffix'")
        );
    }

    @ParameterizedTest(name = "{0}: name + \"suffix\"")
    @MethodSource("stringConcatTests")
    @DisplayName("String concatenation across dialects")
    void testStringConcat(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("name + \"suffix\"", dialect)).isEqualTo(expectedSql);
    }

    // ---- Ternary / conditional ----

    static Stream<Arguments> ternaryTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "CASE WHEN adult THEN 'yes' ELSE 'no' END"),
            Arguments.of("MySQL", new MySqlDialect(), "CASE WHEN adult THEN 'yes' ELSE 'no' END"),
            Arguments.of("SQLite", new SqliteDialect(), "CASE WHEN adult THEN 'yes' ELSE 'no' END"),
            Arguments.of("DuckDB", new DuckDbDialect(), "CASE WHEN adult THEN 'yes' ELSE 'no' END"),
            Arguments.of("BigQuery", new BigQueryDialect(), "CASE WHEN adult THEN 'yes' ELSE 'no' END")
        );
    }

    @ParameterizedTest(name = "{0}: adult ? \"yes\" : \"no\"")
    @MethodSource("ternaryTests")
    @DisplayName("Ternary expression across dialects")
    void testTernary(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("adult ? \"yes\" : \"no\"", dialect)).isEqualTo(expectedSql);
    }

    // ---- NULL handling ----

    static Stream<Arguments> nullTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "null_var IS NULL"),
            Arguments.of("MySQL", new MySqlDialect(), "null_var IS NULL"),
            Arguments.of("SQLite", new SqliteDialect(), "null_var IS NULL"),
            Arguments.of("DuckDB", new DuckDbDialect(), "null_var IS NULL"),
            Arguments.of("BigQuery", new BigQueryDialect(), "null_var IS NULL")
        );
    }

    @ParameterizedTest(name = "{0}: null_var == null")
    @MethodSource("nullTests")
    @DisplayName("NULL handling across dialects")
    void testNull(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("null_var == null", dialect)).isEqualTo(expectedSql);
    }

    // ---- NOT operator ----

    static Stream<Arguments> notTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "NOT adult"),
            Arguments.of("MySQL", new MySqlDialect(), "NOT adult"),
            Arguments.of("SQLite", new SqliteDialect(), "NOT adult"),
            Arguments.of("DuckDB", new DuckDbDialect(), "NOT adult"),
            Arguments.of("BigQuery", new BigQueryDialect(), "NOT adult")
        );
    }

    @ParameterizedTest(name = "{0}: !adult")
    @MethodSource("notTests")
    @DisplayName("NOT operator across dialects")
    void testNot(String dialectName, Dialect dialect, String expectedSql) throws Exception {
        assertThat(convert("!adult", dialect)).isEqualTo(expectedSql);
    }

    // ---- Parameterized queries with different placeholder styles ----

    static Stream<Arguments> parameterizedTests() {
        return Stream.of(
            Arguments.of("PostgreSQL", new PostgresDialect(), "name = $1", 1),
            Arguments.of("MySQL", new MySqlDialect(), "name = ?", 1),
            Arguments.of("SQLite", new SqliteDialect(), "name = ?", 1),
            Arguments.of("DuckDB", new DuckDbDialect(), "name = $1", 1),
            Arguments.of("BigQuery", new BigQueryDialect(), "name = @p1", 1)
        );
    }

    @ParameterizedTest(name = "{0}: parameterized name == \"John\"")
    @MethodSource("parameterizedTests")
    @DisplayName("Parameterized query placeholders across dialects")
    void testParameterized(String dialectName, Dialect dialect, String expectedSql, int expectedParamCount) throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile("name == \"John\"");
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        ConvertResult result = Cel2Sql.convertParameterized(ast, opts);
        assertThat(result.sql()).isEqualTo(expectedSql);
        assertThat(result.parameters()).hasSize(expectedParamCount);
        assertThat(result.parameters().get(0)).isEqualTo("John");
    }
}
