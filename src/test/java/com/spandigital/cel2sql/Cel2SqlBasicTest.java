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
 * Basic conversion tests covering equality, comparison, null handling, boolean
 * logic, negation, and ternary expressions across all 5 SQL dialects.
 * Mirrors the test cases from Go's testcases/basic_tests.go.
 */
class Cel2SqlBasicTest {

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

    static Stream<Arguments> basicTests() {
        return Stream.of(
            allDialects("equality_string", "name == \"a\"", "name = 'a'"),
            allDialects("inequality_int", "age != 20", "age != 20"),
            allDialects("less_than", "age < 20", "age < 20"),
            allDialects("less_equal", "age <= 20", "age <= 20"),
            allDialects("greater_than", "age > 20", "age > 20"),
            allDialects("greater_equal_float", "height >= 1.6180339887", "height >= 1.6180339887"),
            allDialects("is_null", "null_var == null", "null_var IS NULL"),
            allDialects("is_not_null", "null_var != null", "null_var IS NOT NULL"),
            allDialects("is_true", "adult == true", "adult IS TRUE"),
            allDialects("is_not_true", "adult != true", "adult IS NOT TRUE"),
            allDialects("is_false", "adult == false", "adult IS FALSE"),
            allDialects("is_not_false", "adult != false", "adult IS NOT FALSE"),
            allDialects("not", "!adult", "NOT adult"),
            allDialects("negative_int", "-1", "-1"),
            allDialects("negative_var", "-age", "-age"),
            allDialects("ternary", "name == \"a\" ? \"a\" : \"b\"",
                "CASE WHEN name = 'a' THEN 'a' ELSE 'b' END"),
            allDialects("field_select", "page.title == \"test\"", "page.title = 'test'"),
            allDialects("string_literal", "\"hello\"", "'hello'"),
            allDialects("int_literal", "42", "42"),
            allDialects("double_literal", "3.14", "3.14"),
            allDialects("bool_true", "true", "TRUE"),
            allDialects("bool_false", "false", "FALSE")
        ).flatMap(s -> s);
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("basicTests")
    void testBasic(String name, String celExpr, String dialectName, Dialect dialect,
                   String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
