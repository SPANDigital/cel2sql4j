package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Type cast tests covering int(), string(), double(), bool() casts,
 * and special cases like int(timestamp) for epoch extraction.
 * Mirrors the test cases from Go's testcases/cast_tests.go.
 */
class Cel2SqlCastTest {

    static Stream<Arguments> castTests() {
        return Stream.of(
            Arguments.of("cast_int_from_string",
                "int(\"42\") == 42",
                "CAST('42' AS BIGINT) = 42"),
            Arguments.of("cast_string_from_int",
                "string(42) == \"42\"",
                "CAST(42 AS TEXT) = '42'"),
            Arguments.of("cast_int_from_double",
                "int(height)",
                "CAST(height AS BIGINT)"),
            Arguments.of("cast_double_from_int",
                "double(age)",
                "CAST(age AS DOUBLE PRECISION)"),
            Arguments.of("cast_string_from_var",
                "string(age)",
                "CAST(age AS TEXT)"),
            Arguments.of("cast_int_epoch",
                "int(created_at)",
                "EXTRACT(EPOCH FROM created_at)::bigint")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("castTests")
    @DisplayName("Cast PostgreSQL conversions")
    void testCastPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
