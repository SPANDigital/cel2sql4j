package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Operator tests covering logical AND/OR, arithmetic, parenthesization,
 * and string concatenation.
 * Mirrors the test cases from Go's testcases/operator_tests.go.
 */
class Cel2SqlOperatorTest {

    static Stream<Arguments> operatorTests() {
        return Stream.of(
            Arguments.of("logical_and",
                "name == \"a\" && age > 20",
                "name = 'a' AND age > 20"),
            Arguments.of("logical_or",
                "name == \"a\" || age > 20",
                "name = 'a' OR age > 20"),
            Arguments.of("parenthesized_or_inside_and",
                "age >= 10 && (name == \"a\" || name == \"b\")",
                "age >= 10 AND (name = 'a' OR name = 'b')"),
            Arguments.of("parenthesized_and_inside_or",
                "(age >= 10 && name == \"a\") || name == \"b\"",
                "age >= 10 AND name = 'a' OR name = 'b'"),
            Arguments.of("addition",
                "1 + 2 == 3",
                "1 + 2 = 3"),
            Arguments.of("subtraction",
                "10 - 5 == 5",
                "10 - 5 = 5"),
            Arguments.of("multiplication",
                "3 * 4 == 12",
                "3 * 4 = 12"),
            Arguments.of("division",
                "10 / 2 == 5",
                "10 / 2 = 5"),
            Arguments.of("modulo",
                "5 % 3 == 2",
                "5 % 3 = 2"),
            Arguments.of("string_concat",
                "\"a\" + \"b\" == \"ab\"",
                "'a' || 'b' = 'ab'")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("operatorTests")
    @DisplayName("Operator PostgreSQL conversions")
    void testOperatorPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
