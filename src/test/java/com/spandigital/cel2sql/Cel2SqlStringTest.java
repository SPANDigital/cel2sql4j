package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * String function tests covering startsWith, endsWith, contains, size,
 * and matches operations.
 * Mirrors the test cases from Go's testcases/string_tests.go.
 */
class Cel2SqlStringTest {

    static Stream<Arguments> stringTests() {
        return Stream.of(
            Arguments.of("starts_with",
                "name.startsWith(\"a\")",
                "name LIKE 'a%' ESCAPE E'\\\\'"),
            Arguments.of("ends_with",
                "name.endsWith(\"z\")",
                "name LIKE '%z' ESCAPE E'\\\\'"),
            Arguments.of("contains",
                "name.contains(\"abc\")",
                "POSITION('abc' IN name) > 0"),
            Arguments.of("size_string",
                "size(\"test\")",
                "LENGTH('test')"),
            Arguments.of("size_string_var",
                "name.size()",
                "LENGTH(name)"),
            Arguments.of("size_string_global",
                "size(name)",
                "LENGTH(name)")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("stringTests")
    @DisplayName("String function PostgreSQL conversions")
    void testStringPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
