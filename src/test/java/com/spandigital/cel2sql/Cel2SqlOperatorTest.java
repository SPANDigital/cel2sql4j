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
 * Operator tests covering logical AND/OR, arithmetic, parenthesization,
 * and string concatenation across all 5 SQL dialects.
 * Mirrors the test cases from Go's testcases/operator_tests.go.
 */
class Cel2SqlOperatorTest {

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

    static Stream<Arguments> operatorTests() {
        return Stream.of(
            allDialects("logical_and",
                "name == \"a\" && age > 20",
                "name = 'a' AND age > 20"),
            allDialects("logical_or",
                "name == \"a\" || age > 20",
                "name = 'a' OR age > 20"),
            allDialects("parenthesized_or_inside_and",
                "age >= 10 && (name == \"a\" || name == \"b\")",
                "age >= 10 AND (name = 'a' OR name = 'b')"),
            allDialects("parenthesized_and_inside_or",
                "(age >= 10 && name == \"a\") || name == \"b\"",
                "age >= 10 AND name = 'a' OR name = 'b'"),
            allDialects("addition", "1 + 2 == 3", "1 + 2 = 3"),
            allDialects("subtraction", "10 - 5 == 5", "10 - 5 = 5"),
            allDialects("multiplication", "3 * 4 == 12", "3 * 4 = 12"),
            allDialects("division", "10 / 2 == 5", "10 / 2 = 5"),
            allDialects("modulo", "5 % 3 == 2", "5 % 3 = 2"),
            // String concatenation: MySQL uses CONCAT(), others use ||
            Stream.of(
                Arguments.of("string_concat", "\"a\" + \"b\" == \"ab\"", "PostgreSQL", PG, "'a' || 'b' = 'ab'"),
                Arguments.of("string_concat", "\"a\" + \"b\" == \"ab\"", "MySQL", MYSQL, "CONCAT('a', 'b') = 'ab'"),
                Arguments.of("string_concat", "\"a\" + \"b\" == \"ab\"", "SQLite", SQLITE, "'a' || 'b' = 'ab'"),
                Arguments.of("string_concat", "\"a\" + \"b\" == \"ab\"", "DuckDB", DUCKDB, "'a' || 'b' = 'ab'"),
                Arguments.of("string_concat", "\"a\" + \"b\" == \"ab\"", "BigQuery", BQ, "'a' || 'b' = 'ab'")
            )
        ).flatMap(s -> s);
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("operatorTests")
    void testOperator(String name, String celExpr, String dialectName, Dialect dialect,
                      String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
