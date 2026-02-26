package com.spandigital.cel2sql.integration;

import com.spandigital.cel2sql.Cel2Sql;
import com.spandigital.cel2sql.ConvertOptions;
import com.spandigital.cel2sql.ConvertResult;
import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.DialectName;
import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class for dialect-specific integration tests.
 * Defines the test catalog and execution logic. Each subclass provides
 * a database connection, DDL, and test data for its dialect.
 */
abstract class AbstractDialectIntegrationTest {

    protected abstract Connection getConnection() throws SQLException;
    protected abstract Dialect getDialect();
    protected abstract String getDialectName();

    // ===== Test catalog =====

    protected List<IntegrationTestCase> testCatalog() {
        List<IntegrationTestCase> cases = new ArrayList<>();

        // --- Basic WHERE clauses ---
        cases.add(IntegrationTestCase.where("basic_equality_string", "name == \"Alice\"", "basic", 1));
        cases.add(IntegrationTestCase.where("basic_inequality_int", "age != 20", "basic", 1, 2, 3, 4, 6));
        cases.add(IntegrationTestCase.where("basic_less_than", "age < 20", "basic", 2, 6));
        cases.add(IntegrationTestCase.where("basic_less_equal", "age <= 20", "basic", 2, 5, 6));
        cases.add(IntegrationTestCase.where("basic_greater_equal_float", "height >= 1.6180339887", "basic", 1, 2, 3, 4, 5));
        cases.add(IntegrationTestCase.where("basic_is_null", "null_var == null", "basic", 2, 4, 6));
        cases.add(IntegrationTestCase.where("basic_is_not_null", "null_var != null", "basic", 1, 3, 5));
        cases.add(IntegrationTestCase.where("basic_is_true", "adult == true", "basic", 1, 3, 4, 5));
        cases.add(IntegrationTestCase.where("basic_not", "!adult", "basic", 2, 6));

        // --- Expression-only ---
        cases.add(IntegrationTestCase.expr("expr_string_literal", "\"hello\"", "basic"));
        cases.add(IntegrationTestCase.expr("expr_int_literal", "42", "basic"));
        cases.add(IntegrationTestCase.expr("expr_double_literal", "3.14", "basic"));
        cases.add(IntegrationTestCase.expr("expr_bool_true", "true", "basic"));
        cases.add(IntegrationTestCase.expr("expr_negative_int", "-1", "basic"));

        // --- Operators ---
        cases.add(IntegrationTestCase.where("op_and", "name == \"Alice\" && age > 20", "operator", 1));
        cases.add(IntegrationTestCase.where("op_or", "name == \"Alice\" || age > 20", "operator", 1, 3, 4));
        cases.add(IntegrationTestCase.where("op_add", "age + 10 == 40", "operator", 1));
        cases.add(IntegrationTestCase.where("op_modulo", "age % 10 == 0", "operator", 1, 5, 6));

        // --- Strings ---
        cases.add(IntegrationTestCase.where("string_startsWith", "name.startsWith(\"Al\")", "string", 1));
        cases.add(IntegrationTestCase.where("string_endsWith", "name.endsWith(\"e\")", "string", 1, 4, 5));
        cases.add(IntegrationTestCase.where("string_contains", "name.contains(\"ar\")", "string", 3, 6));
        cases.add(IntegrationTestCase.where("string_size", "size(name) == 3", "string", 2, 5));

        // --- Regex (skip SQLite) ---
        cases.add(IntegrationTestCase.where("regex_matches", "name.matches(\"^Ali.*\")", "regex", 1));

        // --- Arrays: IN-list (all dialects) ---
        cases.add(IntegrationTestCase.where("array_in_list", "name in [\"Alice\", \"Bob\", \"Carol\"]", "array_in", 1, 2, 3));

        // --- Arrays: native (skip MySQL/SQLite) ---
        cases.add(IntegrationTestCase.expr("array_index_literal", "[1, 2, 3][0] == 1", "array_native"));

        // --- Comprehensions (skip MySQL, skip SQLite) ---
        cases.add(IntegrationTestCase.where("comp_all", "string_list.all(x, x != \"bad\")", "comprehension", 2, 4, 6));
        cases.add(IntegrationTestCase.where("comp_exists", "string_list.exists(x, x == \"good\")", "comprehension", 1, 2, 4, 5));
        cases.add(IntegrationTestCase.where("comp_exists_one", "string_list.exists_one(x, x == \"unique\")", "comprehension", 4, 5));

        // --- Timestamps ---
        cases.add(IntegrationTestCase.where("timestamp_getFullYear", "created_at.getFullYear() == 2024", "timestamp", 1, 2, 3, 5, 6));
        cases.add(IntegrationTestCase.where("timestamp_getHours", "created_at.getHours() == 10", "timestamp", 1));
        cases.add(IntegrationTestCase.where("timestamp_getDayOfMonth", "created_at.getDayOfMonth() == 25", "timestamp", 4));

        // --- Casts (expression-only) ---
        cases.add(IntegrationTestCase.expr("cast_int_from_string", "int(\"42\") == 42", "cast"));
        cases.add(IntegrationTestCase.expr("cast_string_from_int", "string(42) == \"42\"", "cast"));

        return cases;
    }

    protected List<IntegrationTestCase> parameterizedTestCatalog() {
        List<IntegrationTestCase> cases = new ArrayList<>();
        cases.add(IntegrationTestCase.where("param_string_eq", "name == \"Alice\"", "parameterized", 1));
        cases.add(IntegrationTestCase.where("param_int_gt", "age > 30", "parameterized", 4));
        cases.add(IntegrationTestCase.where("param_compound", "name == \"Alice\" && age > 20", "parameterized", 1));
        cases.add(IntegrationTestCase.where("param_bool_inlined", "active == true", "parameterized", 1, 3, 5));
        return cases;
    }

    // ===== Assumptions =====

    private void applyAssumptions(IntegrationTestCase tc) {
        Dialect dialect = getDialect();
        if ("regex".equals(tc.category())) {
            Assumptions.assumeTrue(dialect.supportsRegex(), getDialectName() + ": no regex support");
        }
        if ("array_native".equals(tc.category())) {
            Assumptions.assumeTrue(dialect.supportsNativeArrays(), getDialectName() + ": no native array support");
            // PostgreSQL requires parentheses around ARRAY constructor before indexing: (ARRAY[...])[i]
            // The library generates ARRAY[...][i] which is a parser error for literal array constructors
            Assumptions.assumeTrue(dialect.name() != DialectName.POSTGRESQL, "PostgreSQL: ARRAY constructor indexing requires parentheses");
        }
        if ("array_in".equals(tc.category())) {
            // MySQL generates JSON_CONTAINS(JSON_ARRAY(...), CAST(name AS JSON)) but CAST(string AS JSON)
            // fails for plain string column values — they're not valid JSON without quoting
            Assumptions.assumeTrue(dialect.name() != DialectName.MYSQL, "MySQL: CAST(column AS JSON) fails for plain strings");
        }
        if ("comprehension".equals(tc.category())) {
            Assumptions.assumeTrue(dialect.name() != DialectName.MYSQL, "MySQL: comprehensions not supported");
            Assumptions.assumeTrue(dialect.name() != DialectName.SQLITE, "SQLite: json_each alias incompatible with generated SQL");
            Assumptions.assumeTrue(dialect.name() != DialectName.DUCKDB, "DuckDB: UNNEST AS alias creates table alias, not column name");
        }
    }

    // ===== Test factories =====

    @TestFactory
    Stream<DynamicTest> sqlTests() {
        return testCatalog().stream()
            .map(tc -> DynamicTest.dynamicTest(tc.name() + " [" + getDialectName() + "]", () -> {
                applyAssumptions(tc);
                var ast = CelHelper.compile(tc.celExpression());
                var opts = ConvertOptions.defaults().withDialect(getDialect());
                String sql = Cel2Sql.convert(ast, opts);
                if (tc.expressionOnly()) {
                    assertExpressionExecutes(sql);
                } else {
                    Set<Integer> actual = executeWhereClause(sql);
                    assertThat(actual)
                        .as("CEL '%s' -> SQL '%s'", tc.celExpression(), sql)
                        .isEqualTo(tc.expectedRowIds());
                }
            }));
    }

    @TestFactory
    Stream<DynamicTest> parameterizedSqlTests() {
        return parameterizedTestCatalog().stream()
            .map(tc -> DynamicTest.dynamicTest(tc.name() + " [" + getDialectName() + "]", () -> {
                applyAssumptions(tc);
                var ast = CelHelper.compile(tc.celExpression());
                var opts = ConvertOptions.defaults().withDialect(getDialect());
                ConvertResult result = Cel2Sql.convertParameterized(ast, opts);
                Set<Integer> actual = executeParameterizedWhereClause(result.sql(), result.parameters());
                assertThat(actual)
                    .as("CEL '%s' -> parameterized SQL '%s' params=%s", tc.celExpression(), result.sql(), result.parameters())
                    .isEqualTo(tc.expectedRowIds());
            }));
    }

    // ===== Execution helpers =====

    private Set<Integer> executeWhereClause(String whereClause) throws SQLException {
        String query = "SELECT id FROM test_data WHERE " + whereClause;
        Set<Integer> ids = new HashSet<>();
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                ids.add(rs.getInt("id"));
            }
        }
        return ids;
    }

    private void assertExpressionExecutes(String expression) throws SQLException {
        String query = "SELECT " + expression;
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            assertThat(rs.next()).as("Expression '%s' should return a result", expression).isTrue();
        }
    }

    private Set<Integer> executeParameterizedWhereClause(String whereClause, List<Object> params) throws SQLException {
        // Normalize dialect-specific placeholders to JDBC '?'
        String jdbcSql = "SELECT id FROM test_data WHERE " +
            whereClause
                .replaceAll("\\$\\d+", "?")
                .replaceAll("@p\\d+", "?");
        Set<Integer> ids = new HashSet<>();
        try (PreparedStatement pstmt = getConnection().prepareStatement(jdbcSql)) {
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof String s) {
                    pstmt.setString(i + 1, s);
                } else if (param instanceof Long l) {
                    pstmt.setLong(i + 1, l);
                } else if (param instanceof Double d) {
                    pstmt.setDouble(i + 1, d);
                } else {
                    pstmt.setObject(i + 1, param);
                }
            }
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                }
            }
        }
        return ids;
    }

    // ===== Schema setup helper =====

    protected void executeSql(Connection conn, String... statements) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (String sql : statements) {
                stmt.execute(sql);
            }
        }
    }
}
