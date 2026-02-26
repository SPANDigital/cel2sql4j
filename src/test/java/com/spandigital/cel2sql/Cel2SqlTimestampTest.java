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
 * Timestamp and duration tests covering duration parsing, timestamp
 * component extraction, and timestamp arithmetic across all 5 dialects.
 * Mirrors the test cases from Go's testcases/timestamp_tests.go.
 */
class Cel2SqlTimestampTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    static Stream<Arguments> timestampTests() {
        return Stream.of(
            // duration_second: PG/MySQL/DuckDB/BQ use INTERVAL, SQLite uses string
            Stream.of(
                Arguments.of("duration_second", "duration(\"10s\")", "PostgreSQL", PG, "INTERVAL 10 SECOND"),
                Arguments.of("duration_second", "duration(\"10s\")", "MySQL", MYSQL, "INTERVAL 10 SECOND"),
                Arguments.of("duration_second", "duration(\"10s\")", "SQLite", SQLITE, "'+10 seconds'"),
                Arguments.of("duration_second", "duration(\"10s\")", "DuckDB", DUCKDB, "INTERVAL 10 SECOND"),
                Arguments.of("duration_second", "duration(\"10s\")", "BigQuery", BQ, "INTERVAL 10 SECOND")
            ),
            Stream.of(
                Arguments.of("duration_minute", "duration(\"1h1m\")", "PostgreSQL", PG, "INTERVAL 61 MINUTE"),
                Arguments.of("duration_minute", "duration(\"1h1m\")", "MySQL", MYSQL, "INTERVAL 61 MINUTE"),
                Arguments.of("duration_minute", "duration(\"1h1m\")", "SQLite", SQLITE, "'+61 minutes'"),
                Arguments.of("duration_minute", "duration(\"1h1m\")", "DuckDB", DUCKDB, "INTERVAL 61 MINUTE"),
                Arguments.of("duration_minute", "duration(\"1h1m\")", "BigQuery", BQ, "INTERVAL 61 MINUTE")
            ),
            Stream.of(
                Arguments.of("duration_hour", "duration(\"60m\")", "PostgreSQL", PG, "INTERVAL 1 HOUR"),
                Arguments.of("duration_hour", "duration(\"60m\")", "MySQL", MYSQL, "INTERVAL 1 HOUR"),
                Arguments.of("duration_hour", "duration(\"60m\")", "SQLite", SQLITE, "'+1 hours'"),
                Arguments.of("duration_hour", "duration(\"60m\")", "DuckDB", DUCKDB, "INTERVAL 1 HOUR"),
                Arguments.of("duration_hour", "duration(\"60m\")", "BigQuery", BQ, "INTERVAL 1 HOUR")
            ),
            // getSeconds: EXTRACT vs strftime
            Stream.of(
                Arguments.of("timestamp_getSeconds", "created_at.getSeconds()", "PostgreSQL", PG,
                    "EXTRACT(SECOND FROM created_at)"),
                Arguments.of("timestamp_getSeconds", "created_at.getSeconds()", "MySQL", MYSQL,
                    "EXTRACT(SECOND FROM created_at)"),
                Arguments.of("timestamp_getSeconds", "created_at.getSeconds()", "SQLite", SQLITE,
                    "CAST(strftime('%S', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getSeconds", "created_at.getSeconds()", "DuckDB", DUCKDB,
                    "EXTRACT(SECOND FROM created_at)"),
                Arguments.of("timestamp_getSeconds", "created_at.getSeconds()", "BigQuery", BQ,
                    "EXTRACT(SECOND FROM created_at)")
            ),
            Stream.of(
                Arguments.of("timestamp_getMinutes", "created_at.getMinutes()", "PostgreSQL", PG,
                    "EXTRACT(MINUTE FROM created_at)"),
                Arguments.of("timestamp_getMinutes", "created_at.getMinutes()", "MySQL", MYSQL,
                    "EXTRACT(MINUTE FROM created_at)"),
                Arguments.of("timestamp_getMinutes", "created_at.getMinutes()", "SQLite", SQLITE,
                    "CAST(strftime('%M', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getMinutes", "created_at.getMinutes()", "DuckDB", DUCKDB,
                    "EXTRACT(MINUTE FROM created_at)"),
                Arguments.of("timestamp_getMinutes", "created_at.getMinutes()", "BigQuery", BQ,
                    "EXTRACT(MINUTE FROM created_at)")
            ),
            Stream.of(
                Arguments.of("timestamp_getHours", "created_at.getHours()", "PostgreSQL", PG,
                    "EXTRACT(HOUR FROM created_at)"),
                Arguments.of("timestamp_getHours", "created_at.getHours()", "MySQL", MYSQL,
                    "EXTRACT(HOUR FROM created_at)"),
                Arguments.of("timestamp_getHours", "created_at.getHours()", "SQLite", SQLITE,
                    "CAST(strftime('%H', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getHours", "created_at.getHours()", "DuckDB", DUCKDB,
                    "EXTRACT(HOUR FROM created_at)"),
                Arguments.of("timestamp_getHours", "created_at.getHours()", "BigQuery", BQ,
                    "EXTRACT(HOUR FROM created_at)")
            ),
            Stream.of(
                Arguments.of("timestamp_getFullYear", "created_at.getFullYear()", "PostgreSQL", PG,
                    "EXTRACT(YEAR FROM created_at)"),
                Arguments.of("timestamp_getFullYear", "created_at.getFullYear()", "MySQL", MYSQL,
                    "EXTRACT(YEAR FROM created_at)"),
                Arguments.of("timestamp_getFullYear", "created_at.getFullYear()", "SQLite", SQLITE,
                    "CAST(strftime('%Y', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getFullYear", "created_at.getFullYear()", "DuckDB", DUCKDB,
                    "EXTRACT(YEAR FROM created_at)"),
                Arguments.of("timestamp_getFullYear", "created_at.getFullYear()", "BigQuery", BQ,
                    "EXTRACT(YEAR FROM created_at)")
            ),
            Stream.of(
                Arguments.of("timestamp_getMonth", "created_at.getMonth()", "PostgreSQL", PG,
                    "EXTRACT(MONTH FROM created_at)"),
                Arguments.of("timestamp_getMonth", "created_at.getMonth()", "MySQL", MYSQL,
                    "EXTRACT(MONTH FROM created_at)"),
                Arguments.of("timestamp_getMonth", "created_at.getMonth()", "SQLite", SQLITE,
                    "CAST(strftime('%m', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getMonth", "created_at.getMonth()", "DuckDB", DUCKDB,
                    "EXTRACT(MONTH FROM created_at)"),
                Arguments.of("timestamp_getMonth", "created_at.getMonth()", "BigQuery", BQ,
                    "EXTRACT(MONTH FROM created_at)")
            ),
            Stream.of(
                Arguments.of("timestamp_getDayOfMonth", "created_at.getDayOfMonth()", "PostgreSQL", PG,
                    "EXTRACT(DAY FROM created_at)"),
                Arguments.of("timestamp_getDayOfMonth", "created_at.getDayOfMonth()", "MySQL", MYSQL,
                    "EXTRACT(DAY FROM created_at)"),
                Arguments.of("timestamp_getDayOfMonth", "created_at.getDayOfMonth()", "SQLite", SQLITE,
                    "CAST(strftime('%d', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getDayOfMonth", "created_at.getDayOfMonth()", "DuckDB", DUCKDB,
                    "EXTRACT(DAY FROM created_at)"),
                Arguments.of("timestamp_getDayOfMonth", "created_at.getDayOfMonth()", "BigQuery", BQ,
                    "EXTRACT(DAY FROM created_at)")
            ),
            // getDayOfWeek: special handling per dialect
            Stream.of(
                Arguments.of("timestamp_getDayOfWeek", "created_at.getDayOfWeek()", "PostgreSQL", PG,
                    "(EXTRACT(DOW FROM created_at) + 6) % 7"),
                Arguments.of("timestamp_getDayOfWeek", "created_at.getDayOfWeek()", "MySQL", MYSQL,
                    "(DAYOFWEEK(created_at) + 5) % 7"),
                Arguments.of("timestamp_getDayOfWeek", "created_at.getDayOfWeek()", "SQLite", SQLITE,
                    "CAST(strftime('%w', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getDayOfWeek", "created_at.getDayOfWeek()", "DuckDB", DUCKDB,
                    "(EXTRACT(DOW FROM created_at) + 6) % 7"),
                Arguments.of("timestamp_getDayOfWeek", "created_at.getDayOfWeek()", "BigQuery", BQ,
                    "(EXTRACT(DAYOFWEEK FROM created_at) - 1)")
            ),
            // getDayOfYear: EXTRACT(DOY) vs strftime
            Stream.of(
                Arguments.of("timestamp_getDayOfYear", "created_at.getDayOfYear()", "PostgreSQL", PG,
                    "EXTRACT(DOY FROM created_at)"),
                Arguments.of("timestamp_getDayOfYear", "created_at.getDayOfYear()", "MySQL", MYSQL,
                    "EXTRACT(DOY FROM created_at)"),
                Arguments.of("timestamp_getDayOfYear", "created_at.getDayOfYear()", "SQLite", SQLITE,
                    "CAST(strftime('%j', created_at) AS INTEGER)"),
                Arguments.of("timestamp_getDayOfYear", "created_at.getDayOfYear()", "DuckDB", DUCKDB,
                    "EXTRACT(DOY FROM created_at)"),
                Arguments.of("timestamp_getDayOfYear", "created_at.getDayOfYear()", "BigQuery", BQ,
                    "EXTRACT(DOY FROM created_at)")
            )
        ).flatMap(s -> s);
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("timestampTests")
    void testTimestamp(String name, String celExpr, String dialectName, Dialect dialect,
                      String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
