package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.IndexAdvisor;
import com.spandigital.cel2sql.dialect.IndexRecommendation;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.error.ConversionException;
import dev.cel.common.CelAbstractSyntaxTree;

import java.util.List;
import java.util.function.Consumer;

/**
 * Public API for converting CEL (Common Expression Language) expressions to SQL WHERE clauses.
 * Provides two conversion modes: inline literals and parameterized queries.
 *
 * <p>Usage:
 * <pre>{@code
 * // Simple inline conversion
 * String sql = Cel2Sql.convert(ast);
 *
 * // Parameterized conversion
 * ConvertResult result = Cel2Sql.convertParameterized(ast);
 * String sql = result.sql();           // "age = $1"
 * List<Object> params = result.parameters(); // [18]
 *
 * // With options
 * String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new PostgresDialect()));
 * }</pre>
 *
 * <p>Ported from Go's {@code cel2sql.Convert()} and {@code cel2sql.ConvertParameterized()} functions.</p>
 */
public final class Cel2Sql {

    private Cel2Sql() {}

    /**
     * Converts a CEL AST to a SQL WHERE clause with inline literal values.
     *
     * @param ast     the checked CEL abstract syntax tree
     * @param options zero or more option configurators
     * @return the SQL WHERE clause string
     * @throws ConversionException if the expression cannot be converted
     */
    @SafeVarargs
    public static String convert(CelAbstractSyntaxTree ast, Consumer<ConvertOptions>... options) throws ConversionException {
        ConvertOptions opts = buildOptions(options);
        Converter converter = new Converter(ast, opts, false);
        return converter.convert();
    }

    /**
     * Converts a CEL AST to a SQL WHERE clause with a pre-built ConvertOptions instance.
     *
     * @param ast     the checked CEL abstract syntax tree
     * @param opts    the conversion options
     * @return the SQL WHERE clause string
     * @throws ConversionException if the expression cannot be converted
     */
    public static String convert(CelAbstractSyntaxTree ast, ConvertOptions opts) throws ConversionException {
        if (opts.dialect() == null) {
            opts.withDialect(new PostgresDialect());
        }
        Converter converter = new Converter(ast, opts, false);
        return converter.convert();
    }

    /**
     * Converts a CEL AST to a parameterized SQL WHERE clause.
     * Literal values are replaced with placeholders ($1, $2, etc. for PostgreSQL)
     * and returned separately. Booleans and nulls are always inlined for query
     * plan optimization.
     *
     * @param ast     the checked CEL abstract syntax tree
     * @param options zero or more option configurators
     * @return a {@link ConvertResult} containing the SQL string and parameter values
     * @throws ConversionException if the expression cannot be converted
     */
    @SafeVarargs
    public static ConvertResult convertParameterized(CelAbstractSyntaxTree ast, Consumer<ConvertOptions>... options) throws ConversionException {
        ConvertOptions opts = buildOptions(options);
        Converter converter = new Converter(ast, opts, true);
        String sql = converter.convert();
        return new ConvertResult(sql, converter.getParameters());
    }

    /**
     * Converts a CEL AST to a parameterized SQL WHERE clause with a pre-built ConvertOptions instance.
     *
     * @param ast  the checked CEL abstract syntax tree
     * @param opts the conversion options
     * @return a {@link ConvertResult} containing the SQL string and parameter values
     * @throws ConversionException if the expression cannot be converted
     */
    public static ConvertResult convertParameterized(CelAbstractSyntaxTree ast, ConvertOptions opts) throws ConversionException {
        if (opts.dialect() == null) {
            opts.withDialect(new PostgresDialect());
        }
        Converter converter = new Converter(ast, opts, true);
        String sql = converter.convert();
        return new ConvertResult(sql, converter.getParameters());
    }

    /**
     * Converts a CEL AST to SQL and provides dialect-specific index recommendations.
     * Analyzes query patterns to suggest indexes that would optimize performance.
     *
     * @param ast     the checked CEL abstract syntax tree
     * @param options zero or more option configurators
     * @return an {@link AnalyzeResult} containing the SQL string and index recommendations
     * @throws ConversionException if the expression cannot be converted
     */
    @SafeVarargs
    public static AnalyzeResult analyzeQuery(CelAbstractSyntaxTree ast, Consumer<ConvertOptions>... options) throws ConversionException {
        ConvertOptions opts = buildOptions(options);
        String sql = convert(ast, opts);

        IndexAdvisor advisor = null;
        if (opts.dialect() instanceof IndexAdvisor a) {
            advisor = a;
        } else {
            advisor = new PostgresDialect();
        }

        Converter converter = new Converter(ast, opts, false);
        List<IndexRecommendation> recommendations = converter.collectIndexRecommendations(advisor);

        return new AnalyzeResult(sql, recommendations);
    }

    /**
     * Converts a CEL AST to SQL and provides dialect-specific index recommendations
     * using pre-built ConvertOptions.
     *
     * @param ast  the checked CEL abstract syntax tree
     * @param opts the conversion options
     * @return an {@link AnalyzeResult} containing the SQL string and index recommendations
     * @throws ConversionException if the expression cannot be converted
     */
    public static AnalyzeResult analyzeQuery(CelAbstractSyntaxTree ast, ConvertOptions opts) throws ConversionException {
        if (opts.dialect() == null) {
            opts.withDialect(new PostgresDialect());
        }
        String sql = convert(ast, opts);

        IndexAdvisor advisor = null;
        if (opts.dialect() instanceof IndexAdvisor a) {
            advisor = a;
        } else {
            advisor = new PostgresDialect();
        }

        Converter converter = new Converter(ast, opts, false);
        List<IndexRecommendation> recommendations = converter.collectIndexRecommendations(advisor);

        return new AnalyzeResult(sql, recommendations);
    }

    @SafeVarargs
    private static ConvertOptions buildOptions(Consumer<ConvertOptions>... options) {
        ConvertOptions opts = ConvertOptions.defaults();
        for (Consumer<ConvertOptions> opt : options) {
            opt.accept(opts);
        }
        if (opts.dialect() == null) {
            opts.withDialect(new PostgresDialect());
        }
        return opts;
    }
}
