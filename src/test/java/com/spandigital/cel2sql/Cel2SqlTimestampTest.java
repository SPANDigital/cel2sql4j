package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Timestamp and duration tests covering duration parsing, timestamp
 * component extraction, and timestamp arithmetic.
 * Mirrors the test cases from Go's testcases/timestamp_tests.go.
 */
class Cel2SqlTimestampTest {

    static Stream<Arguments> timestampTests() {
        return Stream.of(
            Arguments.of("duration_second",
                "duration(\"10s\")",
                "INTERVAL 10 SECOND"),
            Arguments.of("duration_minute",
                "duration(\"1h1m\")",
                "INTERVAL 61 MINUTE"),
            Arguments.of("duration_hour",
                "duration(\"60m\")",
                "INTERVAL 1 HOUR"),
            Arguments.of("timestamp_getSeconds",
                "created_at.getSeconds()",
                "EXTRACT(SECOND FROM created_at)"),
            Arguments.of("timestamp_getMinutes",
                "created_at.getMinutes()",
                "EXTRACT(MINUTE FROM created_at)"),
            Arguments.of("timestamp_getHours",
                "created_at.getHours()",
                "EXTRACT(HOUR FROM created_at)"),
            Arguments.of("timestamp_getFullYear",
                "created_at.getFullYear()",
                "EXTRACT(YEAR FROM created_at)"),
            Arguments.of("timestamp_getMonth",
                "created_at.getMonth()",
                "EXTRACT(MONTH FROM created_at)"),
            Arguments.of("timestamp_getDayOfMonth",
                "created_at.getDayOfMonth()",
                "EXTRACT(DAY FROM created_at)"),
            Arguments.of("timestamp_getDayOfWeek",
                "created_at.getDayOfWeek()",
                "(EXTRACT(DOW FROM created_at) + 6) % 7"),
            Arguments.of("timestamp_getDayOfYear",
                "created_at.getDayOfYear()",
                "EXTRACT(DOY FROM created_at)")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("timestampTests")
    @DisplayName("Timestamp/duration PostgreSQL conversions")
    void testTimestampPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
