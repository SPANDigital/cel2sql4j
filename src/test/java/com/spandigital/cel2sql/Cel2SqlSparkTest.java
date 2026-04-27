package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.spark.SparkDialect;
import com.spandigital.cel2sql.error.ConversionException;
import com.spandigital.cel2sql.testutil.CelHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Apache Spark SQL dialect tests. Test cases covered: RLIKE for regex,
 * {@code array_contains} for membership, {@code size} + {@code COALESCE} for length,
 * and the {@code dayofweek - 1} adjustment for {@code getDayOfWeek}.
 */
class Cel2SqlSparkTest {

    private static final Dialect SPARK = new SparkDialect();

    static Stream<Arguments> sparkBasicTests() {
        return Stream.of(
            // Comparisons + boolean/null inlining behave the same as other dialects.
            Arguments.of("string_eq",       "name == \"Alice\"",         "name = 'Alice'"),
            Arguments.of("int_gt",          "age > 21",                  "age > 21"),
            Arguments.of("bool_true",       "active == true",            "active IS TRUE"),
            Arguments.of("null_check",      "null_var == null",          "null_var IS NULL"),
            Arguments.of("and_logic",       "age > 18 && active",        "age > 18 AND active"),
            Arguments.of("or_logic",        "name == \"a\" || name == \"b\"", "name = 'a' OR name = 'b'"),

            // String functions: concat() / RLIKE / LOCATE / LIKE.
            Arguments.of("concat",          "name + \"_x\"",             "concat(name, '_x')"),
            Arguments.of("contains",        "name.contains(\"li\")",     "LOCATE('li', name) > 0"),
            Arguments.of("starts_with",     "name.startsWith(\"a\")",    "name LIKE 'a%' ESCAPE '\\\\'"),
            Arguments.of("ends_with",       "name.endsWith(\"e\")",      "name LIKE '%e' ESCAPE '\\\\'"),
            Arguments.of("regex_match",     "name.matches(\"^a.*z$\")",  "name RLIKE '^a.*z$'"),

            // Arrays: array literal / array_contains / COALESCE(size).
            Arguments.of("array_literal",   "[1, 2, 3][0] == 1",         "array(1, 2, 3)[0] = 1"),
            Arguments.of("array_membership","\"x\" in tags",             "array_contains(tags, 'x')"),
            Arguments.of("array_size",      "size(string_list)",         "COALESCE(size(string_list), 0)"),
            Arguments.of("array_size_method","string_list.size()",       "COALESCE(size(string_list), 0)"),

            // Timestamps: EXTRACT(... FROM ts), with the dayofweek -1 adjustment.
            Arguments.of("year",            "created_at.getFullYear()",  "EXTRACT(YEAR FROM created_at)"),
            Arguments.of("hour",            "created_at.getHours()",     "EXTRACT(HOUR FROM created_at)"),
            Arguments.of("day_of_week",     "created_at.getDayOfWeek()", "(dayofweek(created_at) - 1)")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sparkBasicTests")
    void testSparkBasic(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(SPARK));
        assertThat(sql).as("%s: CEL '%s'", name, celExpr).isEqualTo(expectedSql);
    }

    @Test
    void parameterizedUsesPositionalQuestionMark() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\" && age > 21");
        ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts.withDialect(SPARK));
        assertThat(res.sql()).isEqualTo("name = ? AND age > ?");
        assertThat(res.parameters()).containsExactly("Alice", 21L);
    }

    @Test
    void analyzeQueryReturnsEmptyRecommendations() throws Exception {
        // Spark indexing is storage-layer specific; the dialect intentionally
        // returns no recommendations so callers don't get bogus Postgres advice.
        var ast = CelHelper.compile("name == \"Alice\" && age > 21");
        AnalyzeResult res = Cel2Sql.analyzeQuery(ast, opts -> opts.withDialect(SPARK));
        assertThat(res.sql()).isEqualTo("name = 'Alice' AND age > 21");
        assertThat(res.recommendations()).isEmpty();
    }

    @Test
    void reservedKeywordRejectedAsFieldName() {
        // Spark identifier validation rejects reserved keywords (defense in depth).
        SparkDialect spark = new SparkDialect();
        assertThatThrownBy(() -> spark.validateFieldName("select"))
                .isInstanceOf(ConversionException.class);
        assertThatThrownBy(() -> spark.validateFieldName("not"))
                .isInstanceOf(ConversionException.class);
    }

    @Test
    void invalidIdentifierShapeRejected() {
        SparkDialect spark = new SparkDialect();
        assertThatThrownBy(() -> spark.validateFieldName("1bad"))
                .isInstanceOf(ConversionException.class);
        assertThatThrownBy(() -> spark.validateFieldName("name space"))
                .isInstanceOf(ConversionException.class);
        assertThatThrownBy(() -> spark.validateFieldName(""))
                .isInstanceOf(ConversionException.class);
    }

    @Test
    void multiDimensionalArrayLengthIsRejected() throws Exception {
        // Spark does not support multi-dim ARRAY_LENGTH; the dialect should
        // throw rather than emit invalid SQL.
        SparkDialect spark = new SparkDialect();
        StringBuilder sb = new StringBuilder();
        assertThatThrownBy(() -> spark.writeArrayLength(sb, 2, () -> sb.append("x")))
                .isInstanceOf(ConversionException.class);
    }

    @Test
    void regexPassesThroughAndRejectsLookahead() throws Exception {
        SparkDialect spark = new SparkDialect();
        // Pass-through: Java regex == Spark regex, so the pattern is unchanged.
        var ok = spark.convertRegex("^[a-z]+$");
        assertThat(ok.pattern()).isEqualTo("^[a-z]+$");

        // Lookahead is rejected (RE2 doesn't support it; defense in depth).
        assertThatThrownBy(() -> spark.convertRegex("(?=foo)bar"))
                .isInstanceOf(ConversionException.class);
    }

    @Test
    void regexCaseInsensitiveInlineFlagIsHonouredByEngine() throws Exception {
        // The (?i) prefix is left in the pattern; Spark's engine honours it natively,
        // so the dialect reports caseInsensitive=false (no separate ~* operator).
        SparkDialect spark = new SparkDialect();
        var res = spark.convertRegex("(?i)Hello");
        assertThat(res.pattern()).isEqualTo("(?i)Hello");
        assertThat(res.caseInsensitive()).isFalse();
    }

    @Test
    void jsonArrayMembershipThrowsClearly() throws Exception {
        // A scalar subquery built from EXPLODE(from_json(...)) can return multiple
        // rows, which Spark rejects at runtime. The dialect throws at conversion
        // time so users get a clear diagnostic instead of an opaque runtime error.
        SparkDialect spark = new SparkDialect();
        StringBuilder sb = new StringBuilder();
        assertThatThrownBy(() -> spark.writeJSONArrayMembership(sb, "any", () -> sb.append("x")))
                .isInstanceOf(ConversionException.class);
        StringBuilder sb2 = new StringBuilder();
        assertThatThrownBy(() -> spark.writeNestedJSONArrayMembership(sb2, () -> sb2.append("x")))
                .isInstanceOf(ConversionException.class);
    }

    static Stream<Arguments> sparkInListTests() {
        return Stream.of(
            Arguments.of("string_in_literal",
                    "name in [\"a\", \"b\", \"c\"]",
                    "array_contains(array('a', 'b', 'c'), name)"),
            Arguments.of("int_in_literal",
                    "age in [1, 2, 3]",
                    "array_contains(array(1, 2, 3), age)")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("sparkInListTests")
    void testSparkInList(String name, String celExpr, String expectedSql) throws Exception {
        var ast = CelHelper.compile(celExpr);
        String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(SPARK));
        assertThat(sql).as("%s: CEL '%s'", name, celExpr).isEqualTo(expectedSql);
    }
}
