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
 * Type cast tests covering int(), string(), double(), bool() casts,
 * and special cases like int(timestamp) for epoch extraction across all 5 dialects.
 * Mirrors the test cases from Go's testcases/cast_tests.go.
 */
class Cel2SqlCastTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    static Stream<Arguments> castTests() {
        return Stream.of(
            // cast_int_from_string: int type name varies per dialect
            Stream.of(
                Arguments.of("cast_int_from_string", "int(\"42\") == 42", "PostgreSQL", PG,
                    "CAST('42' AS BIGINT) = 42"),
                Arguments.of("cast_int_from_string", "int(\"42\") == 42", "MySQL", MYSQL,
                    "CAST('42' AS SIGNED) = 42"),
                Arguments.of("cast_int_from_string", "int(\"42\") == 42", "SQLite", SQLITE,
                    "CAST('42' AS INTEGER) = 42"),
                Arguments.of("cast_int_from_string", "int(\"42\") == 42", "DuckDB", DUCKDB,
                    "CAST('42' AS BIGINT) = 42"),
                Arguments.of("cast_int_from_string", "int(\"42\") == 42", "BigQuery", BQ,
                    "CAST('42' AS INT64) = 42")
            ),
            // cast_string_from_int: string type name varies per dialect
            Stream.of(
                Arguments.of("cast_string_from_int", "string(42) == \"42\"", "PostgreSQL", PG,
                    "CAST(42 AS TEXT) = '42'"),
                Arguments.of("cast_string_from_int", "string(42) == \"42\"", "MySQL", MYSQL,
                    "CAST(42 AS CHAR) = '42'"),
                Arguments.of("cast_string_from_int", "string(42) == \"42\"", "SQLite", SQLITE,
                    "CAST(42 AS TEXT) = '42'"),
                Arguments.of("cast_string_from_int", "string(42) == \"42\"", "DuckDB", DUCKDB,
                    "CAST(42 AS VARCHAR) = '42'"),
                Arguments.of("cast_string_from_int", "string(42) == \"42\"", "BigQuery", BQ,
                    "CAST(42 AS STRING) = '42'")
            ),
            // cast_int_from_double: int type name varies per dialect
            Stream.of(
                Arguments.of("cast_int_from_double", "int(height)", "PostgreSQL", PG,
                    "CAST(height AS BIGINT)"),
                Arguments.of("cast_int_from_double", "int(height)", "MySQL", MYSQL,
                    "CAST(height AS SIGNED)"),
                Arguments.of("cast_int_from_double", "int(height)", "SQLite", SQLITE,
                    "CAST(height AS INTEGER)"),
                Arguments.of("cast_int_from_double", "int(height)", "DuckDB", DUCKDB,
                    "CAST(height AS BIGINT)"),
                Arguments.of("cast_int_from_double", "int(height)", "BigQuery", BQ,
                    "CAST(height AS INT64)")
            ),
            // cast_double_from_int: double type name varies per dialect
            Stream.of(
                Arguments.of("cast_double_from_int", "double(age)", "PostgreSQL", PG,
                    "CAST(age AS DOUBLE PRECISION)"),
                Arguments.of("cast_double_from_int", "double(age)", "MySQL", MYSQL,
                    "CAST(age AS DECIMAL)"),
                Arguments.of("cast_double_from_int", "double(age)", "SQLite", SQLITE,
                    "CAST(age AS REAL)"),
                Arguments.of("cast_double_from_int", "double(age)", "DuckDB", DUCKDB,
                    "CAST(age AS DOUBLE)"),
                Arguments.of("cast_double_from_int", "double(age)", "BigQuery", BQ,
                    "CAST(age AS FLOAT64)")
            ),
            // cast_string_from_var: string type name varies per dialect
            Stream.of(
                Arguments.of("cast_string_from_var", "string(age)", "PostgreSQL", PG,
                    "CAST(age AS TEXT)"),
                Arguments.of("cast_string_from_var", "string(age)", "MySQL", MYSQL,
                    "CAST(age AS CHAR)"),
                Arguments.of("cast_string_from_var", "string(age)", "SQLite", SQLITE,
                    "CAST(age AS TEXT)"),
                Arguments.of("cast_string_from_var", "string(age)", "DuckDB", DUCKDB,
                    "CAST(age AS VARCHAR)"),
                Arguments.of("cast_string_from_var", "string(age)", "BigQuery", BQ,
                    "CAST(age AS STRING)")
            ),
            // cast_int_epoch: special epoch extraction per dialect
            Stream.of(
                Arguments.of("cast_int_epoch", "int(created_at)", "PostgreSQL", PG,
                    "EXTRACT(EPOCH FROM created_at)::bigint"),
                Arguments.of("cast_int_epoch", "int(created_at)", "MySQL", MYSQL,
                    "UNIX_TIMESTAMP(created_at)"),
                Arguments.of("cast_int_epoch", "int(created_at)", "SQLite", SQLITE,
                    "CAST(strftime('%s', created_at) AS INTEGER)"),
                Arguments.of("cast_int_epoch", "int(created_at)", "DuckDB", DUCKDB,
                    "EXTRACT(EPOCH FROM created_at)::BIGINT"),
                Arguments.of("cast_int_epoch", "int(created_at)", "BigQuery", BQ,
                    "UNIX_SECONDS(created_at)")
            )
        ).flatMap(s -> s);
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("castTests")
    void testCast(String name, String celExpr, String dialectName, Dialect dialect,
                  String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
