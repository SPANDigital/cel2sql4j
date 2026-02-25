package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic conversion tests covering equality, comparison, null handling, boolean
 * logic, negation, and ternary expressions.
 * Mirrors the test cases from Go's testcases/basic_tests.go.
 */
class Cel2SqlBasicTest {

    static Stream<Arguments> basicTests() {
        return Stream.of(
            Arguments.of("equality_string", "name == \"a\"", "name = 'a'"),
            Arguments.of("inequality_int", "age != 20", "age != 20"),
            Arguments.of("less_than", "age < 20", "age < 20"),
            Arguments.of("less_equal", "age <= 20", "age <= 20"),
            Arguments.of("greater_than", "age > 20", "age > 20"),
            Arguments.of("greater_equal_float", "height >= 1.6180339887", "height >= 1.6180339887"),
            Arguments.of("is_null", "null_var == null", "null_var IS NULL"),
            Arguments.of("is_not_null", "null_var != null", "null_var IS NOT NULL"),
            Arguments.of("is_true", "adult == true", "adult IS TRUE"),
            Arguments.of("is_not_true", "adult != true", "adult IS NOT TRUE"),
            Arguments.of("is_false", "adult == false", "adult IS FALSE"),
            Arguments.of("is_not_false", "adult != false", "adult IS NOT FALSE"),
            Arguments.of("not", "!adult", "NOT adult"),
            Arguments.of("negative_int", "-1", "-1"),
            Arguments.of("negative_var", "-age", "-age"),
            Arguments.of("ternary", "name == \"a\" ? \"a\" : \"b\"", "CASE WHEN name = 'a' THEN 'a' ELSE 'b' END"),
            Arguments.of("string_literal", "\"hello\"", "'hello'"),
            Arguments.of("int_literal", "42", "42"),
            Arguments.of("double_literal", "3.14", "3.14"),
            Arguments.of("bool_true", "true", "TRUE"),
            Arguments.of("bool_false", "false", "FALSE")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("basicTests")
    @DisplayName("Basic PostgreSQL conversions")
    void testBasicPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
