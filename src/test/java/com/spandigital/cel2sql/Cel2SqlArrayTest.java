package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Array/list tests covering list indexing, size, and the IN operator.
 * Mirrors the test cases from Go's testcases/array_tests.go.
 */
class Cel2SqlArrayTest {

    static Stream<Arguments> arrayTests() {
        return Stream.of(
            Arguments.of("list_index_literal",
                "[1, 2, 3][0] == 1",
                "ARRAY[1, 2, 3][1] = 1"),
            Arguments.of("size_list",
                "size(string_list)",
                "COALESCE(ARRAY_LENGTH(string_list, 1), 0)"),
            Arguments.of("in_list",
                "name in [\"a\", \"b\", \"c\"]",
                "name = ANY(ARRAY['a', 'b', 'c'])"),
            Arguments.of("size_list_var_method",
                "string_list.size()",
                "COALESCE(ARRAY_LENGTH(string_list, 1), 0)")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("arrayTests")
    @DisplayName("Array PostgreSQL conversions")
    void testArrayPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
