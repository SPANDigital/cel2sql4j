package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.mysql.MySqlDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import com.spandigital.cel2sql.error.ConversionException;
import com.spandigital.cel2sql.testutil.CelHelper;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.extensions.CelExtensions;
import dev.cel.parser.CelStandardMacro;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the convert options ported from upstream cel2sql v3.7.1:
 * {@code withJsonVariables}, {@code withColumnAliases}, {@code withParamStartIndex},
 * plus byte-array length cap and the CEL {@code format()} string function.
 */
class Cel2SqlOptionsTest {

    private static final Dialect PG = new PostgresDialect();
    private static final Dialect MYSQL = new MySqlDialect();
    private static final Dialect SQLITE = new SqliteDialect();
    private static final Dialect DUCKDB = new DuckDbDialect();
    private static final Dialect BQ = new BigQueryDialect();

    // ------------------------------------------------------------------
    // withColumnAliases
    // ------------------------------------------------------------------

    @Test
    void columnAliases_renamesIdentInOutput() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\"");
        String sql = Cel2Sql.convert(ast, opts -> opts
                .withDialect(PG)
                .withColumnAliases(Map.of("name", "usr_name")));
        assertThat(sql).isEqualTo("usr_name = 'Alice'");
    }

    @Test
    void columnAliases_appliesAcrossMultipleIdents() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\" && age > 21");
        String sql = Cel2Sql.convert(ast, opts -> opts
                .withDialect(PG)
                .withColumnAliases(Map.of("name", "usr_name", "age", "usr_age")));
        assertThat(sql).isEqualTo("usr_name = 'Alice' AND usr_age > 21");
    }

    @Test
    void columnAliases_validatedAgainstDialect() throws Exception {
        // An alias value that fails dialect validation must be rejected, not
        // silently emitted (defense-in-depth against alias-driven injection).
        var ast = CelHelper.compile("name == \"Alice\"");
        assertThatThrownBy(() -> Cel2Sql.convert(ast, opts -> opts
                .withDialect(PG)
                .withColumnAliases(Map.of("name", "bad name; DROP TABLE users--"))))
                .isInstanceOf(ConversionException.class);
    }

    // ------------------------------------------------------------------
    // withParamStartIndex
    // ------------------------------------------------------------------

    @Test
    void paramStartIndex_postgresShiftsPlaceholders() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\" && age > 21");
        ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts
                .withDialect(PG)
                .withParamStartIndex(5));
        assertThat(res.sql()).isEqualTo("name = $5 AND age > $6");
        assertThat(res.parameters()).containsExactly("Alice", 21L);
    }

    @Test
    void paramStartIndex_bigqueryShiftsPlaceholders() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\"");
        ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts
                .withDialect(BQ)
                .withParamStartIndex(7));
        assertThat(res.sql()).isEqualTo("name = @p7");
    }

    @Test
    void paramStartIndex_clampedToOne() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\"");
        ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts
                .withDialect(PG)
                .withParamStartIndex(-3));
        assertThat(res.sql()).isEqualTo("name = $1");
    }

    // ------------------------------------------------------------------
    // withJsonVariables
    // ------------------------------------------------------------------

    private static CelCompiler jsonVarCompiler() {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addVar("context", MapType.create(SimpleType.STRING, SimpleType.DYN))
                .addVar("name", SimpleType.STRING)
                .build();
    }

    /**
     * Compiler with the CEL strings extension registered (provides split/join/etc.).
     * cel-java's strings extension does NOT include {@code format()} (cel-go does),
     * so we declare it manually here as a member function on string returning string.
     */
    private static CelCompiler stringsExtCompiler() {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .addVar("name", SimpleType.STRING)
                .addVar("tags", ListType.create(SimpleType.STRING))
                .addLibraries(CelExtensions.strings())
                .addFunctionDeclarations(
                        CelFunctionDecl.newFunctionDeclaration(
                                "format",
                                CelOverloadDecl.newMemberOverload(
                                        "string_format_list",
                                        SimpleType.STRING,
                                        SimpleType.STRING,
                                        ListType.create(SimpleType.DYN))))
                .build();
    }

    @Test
    void jsonVariables_flatColumnEmitsArrowOperator() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile(jsonVarCompiler(), "context.host == \"web-1\"");
        String sql = Cel2Sql.convert(ast, opts -> opts
                .withDialect(PG)
                .withJsonVariables("context"));
        assertThat(sql).isEqualTo("context->>'host' = 'web-1'");
    }

    @Test
    void jsonVariables_unmarkedVarUsesPlainDot() throws Exception {
        CelAbstractSyntaxTree ast = CelHelper.compile(jsonVarCompiler(), "context.host == \"web-1\"");
        String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(PG));
        assertThat(sql).isEqualTo("context.host = 'web-1'");
    }

    // ------------------------------------------------------------------
    // Byte array length cap
    // ------------------------------------------------------------------

    @Test
    void byteArrayLengthCap_inlineModeRejectsLongLiteral() {
        // Build a CEL byte literal larger than the cap (10 000 bytes).
        StringBuilder sb = new StringBuilder("b\"");
        for (int i = 0; i < 10_001; i++) {
            sb.append("\\x41"); // 'A'
        }
        sb.append("\"");
        var ast = CelHelper.compile(sb.toString());
        Throwable thrown = catchConversionException(() -> Cel2Sql.convert(ast, opts -> opts.withDialect(PG)));
        assertThat(thrown).isInstanceOf(ConversionException.class);
        assertThat(((ConversionException) thrown).getInternalDetails())
                .contains("byte literal length")
                .contains("10001");
    }

    @Test
    void byteArrayLengthCap_parameterizedModeBypassesCheck() throws Exception {
        StringBuilder sb = new StringBuilder("b\"");
        for (int i = 0; i < 10_001; i++) {
            sb.append("\\x41");
        }
        sb.append("\"");
        var ast = CelHelper.compile(sb.toString());
        ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts.withDialect(PG));
        // Parameterized mode sends bytes directly to the JDBC driver — no inlining, no cap.
        assertThat(res.sql()).isEqualTo("$1");
        assertThat(res.parameters()).hasSize(1);
        assertThat(((byte[]) res.parameters().get(0))).hasSize(10_001);
    }

    // ------------------------------------------------------------------
    // format()
    // ------------------------------------------------------------------

    static Stream<Arguments> formatTests() {
        return Stream.of(
            // Postgres FORMAT collapses %d/%f to %s for safe coercion.
            Arguments.of("format_string_int", "\"%s is %d\".format([\"John\", 30])",
                    "PostgreSQL", PG, "FORMAT('%s is %s', 'John', 30)"),
            Arguments.of("format_string_int", "\"%s is %d\".format([\"John\", 30])",
                    "BigQuery", BQ, "FORMAT('%s is %d', 'John', 30)"),
            Arguments.of("format_string_int", "\"%s is %d\".format([\"John\", 30])",
                    "SQLite", SQLITE, "printf('%s is %d', 'John', 30)"),
            Arguments.of("format_string_int", "\"%s is %d\".format([\"John\", 30])",
                    "DuckDB", DUCKDB, "printf('%s is %d', 'John', 30)"),
            Arguments.of("format_no_args", "\"hello\".format([])",
                    "PostgreSQL", PG, "FORMAT('hello')"),
            Arguments.of("format_double_percent", "\"100%% sure\".format([])",
                    "PostgreSQL", PG, "FORMAT('100%% sure')")
        );
    }

    @ParameterizedTest(name = "{0} [{2}]")
    @MethodSource("formatTests")
    void testFormat(String name, String celExpr, String dialectName, Dialect dialect,
                    String expectedSql) throws Exception {
        var ast = CelHelper.compile(stringsExtCompiler(), celExpr);
        String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(dialect));
        assertThat(sql).isEqualTo(expectedSql);
    }

    @Test
    void format_mysqlIsExplicitlyUnsupported() {
        var ast = CelHelper.compile(stringsExtCompiler(), "\"%s\".format([\"x\"])");
        assertThatThrownBy(() -> Cel2Sql.convert(ast, opts -> opts.withDialect(MYSQL)))
                .isInstanceOf(ConversionException.class);
    }

    @Test
    void format_unsupportedSpecifierIsRejected() {
        var ast = CelHelper.compile(stringsExtCompiler(), "\"%x\".format([15])");
        Throwable thrown = catchConversionException(() -> Cel2Sql.convert(ast, opts -> opts.withDialect(PG)));
        assertThat(thrown).isInstanceOf(ConversionException.class);
        assertThat(((ConversionException) thrown).getInternalDetails())
                .contains("unsupported specifier");
    }

    @Test
    void format_argCountMismatchIsRejected() {
        var ast = CelHelper.compile(stringsExtCompiler(), "\"%s and %s\".format([\"only one\"])");
        Throwable thrown = catchConversionException(() -> Cel2Sql.convert(ast, opts -> opts.withDialect(PG)));
        assertThat(thrown).isInstanceOf(ConversionException.class);
        assertThat(((ConversionException) thrown).getInternalDetails())
                .contains("argument count mismatch");
    }

    /** Captures any throwable for inspection of cause/internal details. */
    private static Throwable catchConversionException(ThrowingAction action) {
        try {
            action.run();
        } catch (Throwable t) {
            return t;
        }
        return null;
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    @Test
    void format_dynamicFormatStringIsRejected() {
        var ast = CelHelper.compile(stringsExtCompiler(), "name.format([])");
        assertThatThrownBy(() -> Cel2Sql.convert(ast, opts -> opts.withDialect(PG)))
                .isInstanceOf(ConversionException.class);
    }

    // ------------------------------------------------------------------
    // Composing options
    // ------------------------------------------------------------------

    @Test
    void aliases_andParamStartIndex_composeCleanly() throws Exception {
        var ast = CelHelper.compile("name == \"Alice\" && age > 21");
        ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts
                .withDialect(PG)
                .withColumnAliases(Map.of("name", "usr_name"))
                .withParamStartIndex(10));
        assertThat(res.sql()).isEqualTo("usr_name = $10 AND age > $11");
        assertThat(res.parameters()).containsExactly("Alice", 21L);
    }
}
