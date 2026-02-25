package com.spandigital.cel2sql;

import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehension tests covering all(), exists(), exists_one(), filter(), and map()
 * macros on lists.
 * Mirrors the test cases from Go's testcases/comprehension_tests.go.
 */
class Cel2SqlComprehensionTest {

    static Stream<Arguments> comprehensionTests() {
        return Stream.of(
            Arguments.of("all",
                "string_list.all(x, x != \"bad\")",
                "NOT EXISTS (SELECT 1 FROM UNNEST(string_list) AS x WHERE NOT (x != 'bad'))"),
            Arguments.of("exists",
                "string_list.exists(x, x == \"good\")",
                "EXISTS (SELECT 1 FROM UNNEST(string_list) AS x WHERE x = 'good')"),
            Arguments.of("exists_one",
                "string_list.exists_one(x, x == \"unique\")",
                "(SELECT COUNT(*) FROM UNNEST(string_list) AS x WHERE x = 'unique') = 1"),
            Arguments.of("filter",
                "string_list.filter(x, x != \"bad\")",
                "ARRAY(SELECT x FROM UNNEST(string_list) AS x WHERE x != 'bad')"),
            Arguments.of("map_transform",
                "string_list.map(x, x + \"_suffix\")",
                "ARRAY(SELECT x || '_suffix' FROM UNNEST(string_list) AS x)")
        );
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("comprehensionTests")
    @DisplayName("Comprehension PostgreSQL conversions")
    void testComprehensionPostgres(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast);
        assertThat(sql).as("CEL '%s' should convert to SQL '%s'", celExpr, expectedSql)
            .isEqualTo(expectedSql);
    }
}
