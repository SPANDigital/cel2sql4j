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

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parameterized query tests verifying that literal values are extracted into
 * dialect-specific positional parameters while booleans and nulls remain inlined.
 * Placeholder styles: PG=$N, MySQL=?, SQLite=?, DuckDB=$N, BigQuery=@pN.
 * Mirrors the test cases from Go's testcases/parameterized_tests.go.
 */
class Cel2SqlParameterizedTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    static Stream<Arguments> parameterizedTests() {
        return Stream.of(
            // simple_string_equality
            Arguments.of("simple_string_equality", "name == \"John\"", "PostgreSQL", PG,
                "name = $1", List.of("John")),
            Arguments.of("simple_string_equality", "name == \"John\"", "MySQL", MYSQL,
                "name = ?", List.of("John")),
            Arguments.of("simple_string_equality", "name == \"John\"", "SQLite", SQLITE,
                "name = ?", List.of("John")),
            Arguments.of("simple_string_equality", "name == \"John\"", "DuckDB", DUCKDB,
                "name = $1", List.of("John")),
            Arguments.of("simple_string_equality", "name == \"John\"", "BigQuery", BQ,
                "name = @p1", List.of("John")),

            // multiple_params
            Arguments.of("multiple_params", "name == \"John\" && name != \"Jane\"", "PostgreSQL", PG,
                "name = $1 AND name != $2", List.of("John", "Jane")),
            Arguments.of("multiple_params", "name == \"John\" && name != \"Jane\"", "MySQL", MYSQL,
                "name = ? AND name != ?", List.of("John", "Jane")),
            Arguments.of("multiple_params", "name == \"John\" && name != \"Jane\"", "SQLite", SQLITE,
                "name = ? AND name != ?", List.of("John", "Jane")),
            Arguments.of("multiple_params", "name == \"John\" && name != \"Jane\"", "DuckDB", DUCKDB,
                "name = $1 AND name != $2", List.of("John", "Jane")),
            Arguments.of("multiple_params", "name == \"John\" && name != \"Jane\"", "BigQuery", BQ,
                "name = @p1 AND name != @p2", List.of("John", "Jane")),

            // integer_equality
            Arguments.of("integer_equality", "age == 18", "PostgreSQL", PG,
                "age = $1", List.of(18L)),
            Arguments.of("integer_equality", "age == 18", "MySQL", MYSQL,
                "age = ?", List.of(18L)),
            Arguments.of("integer_equality", "age == 18", "SQLite", SQLITE,
                "age = ?", List.of(18L)),
            Arguments.of("integer_equality", "age == 18", "DuckDB", DUCKDB,
                "age = $1", List.of(18L)),
            Arguments.of("integer_equality", "age == 18", "BigQuery", BQ,
                "age = @p1", List.of(18L)),

            // double_equality
            Arguments.of("double_equality", "height == 1.75", "PostgreSQL", PG,
                "height = $1", List.of(1.75)),
            Arguments.of("double_equality", "height == 1.75", "MySQL", MYSQL,
                "height = ?", List.of(1.75)),
            Arguments.of("double_equality", "height == 1.75", "SQLite", SQLITE,
                "height = ?", List.of(1.75)),
            Arguments.of("double_equality", "height == 1.75", "DuckDB", DUCKDB,
                "height = $1", List.of(1.75)),
            Arguments.of("double_equality", "height == 1.75", "BigQuery", BQ,
                "height = @p1", List.of(1.75)),

            // boolean_inline_true: booleans are never parameterized
            Arguments.of("boolean_inline_true", "active == true", "PostgreSQL", PG,
                "active IS TRUE", List.of()),
            Arguments.of("boolean_inline_true", "active == true", "MySQL", MYSQL,
                "active IS TRUE", List.of()),
            Arguments.of("boolean_inline_true", "active == true", "SQLite", SQLITE,
                "active IS TRUE", List.of()),
            Arguments.of("boolean_inline_true", "active == true", "DuckDB", DUCKDB,
                "active IS TRUE", List.of()),
            Arguments.of("boolean_inline_true", "active == true", "BigQuery", BQ,
                "active IS TRUE", List.of()),

            // boolean_inline_false: booleans are never parameterized
            Arguments.of("boolean_inline_false", "active == false", "PostgreSQL", PG,
                "active IS FALSE", List.of()),
            Arguments.of("boolean_inline_false", "active == false", "MySQL", MYSQL,
                "active IS FALSE", List.of()),
            Arguments.of("boolean_inline_false", "active == false", "SQLite", SQLITE,
                "active IS FALSE", List.of()),
            Arguments.of("boolean_inline_false", "active == false", "DuckDB", DUCKDB,
                "active IS FALSE", List.of()),
            Arguments.of("boolean_inline_false", "active == false", "BigQuery", BQ,
                "active IS FALSE", List.of()),

            // null_inline: nulls are never parameterized
            Arguments.of("null_inline", "null_var == null", "PostgreSQL", PG,
                "null_var IS NULL", List.of()),
            Arguments.of("null_inline", "null_var == null", "MySQL", MYSQL,
                "null_var IS NULL", List.of()),
            Arguments.of("null_inline", "null_var == null", "SQLite", SQLITE,
                "null_var IS NULL", List.of()),
            Arguments.of("null_inline", "null_var == null", "DuckDB", DUCKDB,
                "null_var IS NULL", List.of()),
            Arguments.of("null_inline", "null_var == null", "BigQuery", BQ,
                "null_var IS NULL", List.of()),

            // mixed_params_and_inlined
            Arguments.of("mixed_params_and_inlined", "name == \"Alice\" && active == true && age > 21",
                "PostgreSQL", PG, "name = $1 AND active IS TRUE AND age > $2", List.of("Alice", 21L)),
            Arguments.of("mixed_params_and_inlined", "name == \"Alice\" && active == true && age > 21",
                "MySQL", MYSQL, "name = ? AND active IS TRUE AND age > ?", List.of("Alice", 21L)),
            Arguments.of("mixed_params_and_inlined", "name == \"Alice\" && active == true && age > 21",
                "SQLite", SQLITE, "name = ? AND active IS TRUE AND age > ?", List.of("Alice", 21L)),
            Arguments.of("mixed_params_and_inlined", "name == \"Alice\" && active == true && age > 21",
                "DuckDB", DUCKDB, "name = $1 AND active IS TRUE AND age > $2", List.of("Alice", 21L)),
            Arguments.of("mixed_params_and_inlined", "name == \"Alice\" && active == true && age > 21",
                "BigQuery", BQ, "name = @p1 AND active IS TRUE AND age > @p2", List.of("Alice", 21L))
        );
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("parameterizedTests")
    @SuppressWarnings("unchecked")
    void testParameterized(String name, String celExpr, String dialectName, Dialect dialect,
                           String expectedSql, List<Object> expectedParams) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        ConvertResult result = Cel2Sql.convertParameterized(ast, opts);
        assertThat(result.sql()).as("SQL for %s [%s]", name, dialectName)
            .isEqualTo(expectedSql);
        assertThat(result.parameters()).as("Parameters for %s [%s]", name, dialectName)
            .isEqualTo(expectedParams);
    }
}
