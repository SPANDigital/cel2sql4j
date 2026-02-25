package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regex matching tests covering simple patterns, word boundary conversion,
 * and character class conversion from Java/RE2 syntax to PostgreSQL POSIX regex.
 * Mirrors the test cases from Go's testcases/regex_tests.go.
 */
class Cel2SqlRegexTest {

    static Stream<Arguments> regexTests() {
        return Stream.of(
            Arguments.of("simple_match",
                "name.matches(\"a+\")",
                "name ~ 'a+'"),
            Arguments.of("word_boundary",
                "name.matches(\"\\\\btest\\\\b\")",
                "name ~ '\\ytest\\y'"),
            Arguments.of("digit_class",
                "name.matches(\"\\\\d{3}-\\\\d{4}\")",
                "name ~ '[[:digit:]]{3}-[[:digit:]]{4}'")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("regexTests")
    @DisplayName("Regex PostgreSQL conversions")
    void testRegexPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
