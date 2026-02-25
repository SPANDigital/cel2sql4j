package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Parameterized query tests verifying that literal values are extracted into
 * positional parameters ($1, $2, ...) while booleans and nulls remain inlined.
 * Mirrors the test cases from Go's testcases/parameterized_tests.go.
 */
class Cel2SqlParameterizedTest {

    static Stream<Arguments> parameterizedTests() {
        return Stream.of(
            Arguments.of("simple_string_equality",
                "name == \"John\"",
                "name = $1",
                List.of("John")),
            Arguments.of("multiple_params",
                "name == \"John\" && name != \"Jane\"",
                "name = $1 AND name != $2",
                List.of("John", "Jane")),
            Arguments.of("integer_equality",
                "age == 18",
                "age = $1",
                List.of(18L)),
            Arguments.of("double_equality",
                "height == 1.75",
                "height = $1",
                List.of(1.75)),
            Arguments.of("boolean_inline_true",
                "active == true",
                "active IS TRUE",
                List.of()),
            Arguments.of("boolean_inline_false",
                "active == false",
                "active IS FALSE",
                List.of()),
            Arguments.of("null_inline",
                "null_var == null",
                "null_var IS NULL",
                List.of()),
            Arguments.of("mixed_params_and_inlined",
                "name == \"Alice\" && active == true && age > 21",
                "name = $1 AND active IS TRUE AND age > $2",
                List.of("Alice", 21L))
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("parameterizedTests")
    @DisplayName("Parameterized query PostgreSQL conversions")
    void testParameterizedPostgres(String name, String celExpr, String expectedSql,
                                   List<Object> expectedParams) throws Exception {
        var ast = CelHelper.compile(celExpr);
        ConvertResult result = Cel2Sql.convertParameterized(ast);
        assertThat(result.sql()).as("SQL for CEL '%s'", celExpr)
            .isEqualTo(expectedSql);
        assertThat(result.parameters()).as("Parameters for CEL '%s'", celExpr)
            .isEqualTo(expectedParams);
    }
}
