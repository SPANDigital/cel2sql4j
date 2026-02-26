package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehension tests covering all(), exists(), exists_one(), filter(), and map()
 * macros on lists across PostgreSQL, SQLite, DuckDB, and BigQuery.
 * MySQL is skipped (no comprehension support in the Go reference).
 * Mirrors the test cases from Go's testcases/comprehension_tests.go.
 */
class Cel2SqlComprehensionTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    static Stream<Arguments> comprehensionTests() {
        return Stream.of(
            // all: NOT EXISTS with UNNEST/json_each
            Arguments.of("all", "string_list.all(x, x != \"bad\")", "PostgreSQL", PG,
                "NOT EXISTS (SELECT 1 FROM UNNEST(string_list) AS x WHERE NOT (x != 'bad'))"),
            Arguments.of("all", "string_list.all(x, x != \"bad\")", "SQLite", SQLITE,
                "NOT EXISTS (SELECT 1 FROM (SELECT value AS x FROM json_each(string_list)) AS _t WHERE NOT (x != 'bad'))"),
            Arguments.of("all", "string_list.all(x, x != \"bad\")", "DuckDB", DUCKDB,
                "NOT EXISTS (SELECT 1 FROM UNNEST(string_list) AS _t(x) WHERE NOT (x != 'bad'))"),
            Arguments.of("all", "string_list.all(x, x != \"bad\")", "BigQuery", BQ,
                "NOT EXISTS (SELECT 1 FROM UNNEST(string_list) AS x WHERE NOT (x != 'bad'))"),

            // exists: EXISTS with UNNEST/json_each
            Arguments.of("exists", "string_list.exists(x, x == \"good\")", "PostgreSQL", PG,
                "EXISTS (SELECT 1 FROM UNNEST(string_list) AS x WHERE x = 'good')"),
            Arguments.of("exists", "string_list.exists(x, x == \"good\")", "SQLite", SQLITE,
                "EXISTS (SELECT 1 FROM (SELECT value AS x FROM json_each(string_list)) AS _t WHERE x = 'good')"),
            Arguments.of("exists", "string_list.exists(x, x == \"good\")", "DuckDB", DUCKDB,
                "EXISTS (SELECT 1 FROM UNNEST(string_list) AS _t(x) WHERE x = 'good')"),
            Arguments.of("exists", "string_list.exists(x, x == \"good\")", "BigQuery", BQ,
                "EXISTS (SELECT 1 FROM UNNEST(string_list) AS x WHERE x = 'good')"),

            // exists_one: COUNT subquery
            Arguments.of("exists_one", "string_list.exists_one(x, x == \"unique\")", "PostgreSQL", PG,
                "(SELECT COUNT(*) FROM UNNEST(string_list) AS x WHERE x = 'unique') = 1"),
            Arguments.of("exists_one", "string_list.exists_one(x, x == \"unique\")", "SQLite", SQLITE,
                "(SELECT COUNT(*) FROM (SELECT value AS x FROM json_each(string_list)) AS _t WHERE x = 'unique') = 1"),
            Arguments.of("exists_one", "string_list.exists_one(x, x == \"unique\")", "DuckDB", DUCKDB,
                "(SELECT COUNT(*) FROM UNNEST(string_list) AS _t(x) WHERE x = 'unique') = 1"),
            Arguments.of("exists_one", "string_list.exists_one(x, x == \"unique\")", "BigQuery", BQ,
                "(SELECT COUNT(*) FROM UNNEST(string_list) AS x WHERE x = 'unique') = 1"),

            // filter: ARRAY subquery / json_group_array
            Arguments.of("filter", "string_list.filter(x, x != \"bad\")", "PostgreSQL", PG,
                "ARRAY(SELECT x FROM UNNEST(string_list) AS x WHERE x != 'bad')"),
            Arguments.of("filter", "string_list.filter(x, x != \"bad\")", "SQLite", SQLITE,
                "(SELECT json_group_array(x) FROM (SELECT value AS x FROM json_each(string_list)) AS _t WHERE x != 'bad')"),
            Arguments.of("filter", "string_list.filter(x, x != \"bad\")", "DuckDB", DUCKDB,
                "ARRAY(SELECT x FROM UNNEST(string_list) AS _t(x) WHERE x != 'bad')"),
            Arguments.of("filter", "string_list.filter(x, x != \"bad\")", "BigQuery", BQ,
                "ARRAY(SELECT x FROM UNNEST(string_list) AS x WHERE x != 'bad')"),

            // map_transform: ARRAY subquery with transform / json_group_array
            Arguments.of("map_transform", "string_list.map(x, x + \"_suffix\")", "PostgreSQL", PG,
                "ARRAY(SELECT x || '_suffix' FROM UNNEST(string_list) AS x)"),
            Arguments.of("map_transform", "string_list.map(x, x + \"_suffix\")", "SQLite", SQLITE,
                "(SELECT json_group_array(x || '_suffix') FROM (SELECT value AS x FROM json_each(string_list)) AS _t)"),
            Arguments.of("map_transform", "string_list.map(x, x + \"_suffix\")", "DuckDB", DUCKDB,
                "ARRAY(SELECT x || '_suffix' FROM UNNEST(string_list) AS _t(x))"),
            Arguments.of("map_transform", "string_list.map(x, x + \"_suffix\")", "BigQuery", BQ,
                "ARRAY(SELECT x || '_suffix' FROM UNNEST(string_list) AS x)")
        );
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("comprehensionTests")
    void testComprehension(String name, String celExpr, String dialectName, Dialect dialect,
                           String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertOptions opts = ConvertOptions.defaults().withDialect(dialect);
        String sql = Cel2Sql.convert(ast, opts);
        assertThat(sql).as("%s: CEL '%s' [%s]", name, celExpr, dialectName)
            .isEqualTo(expectedSql);
    }
}
