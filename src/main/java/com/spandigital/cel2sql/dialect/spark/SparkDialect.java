package com.spandigital.cel2sql.dialect.spark;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.DialectName;
import com.spandigital.cel2sql.dialect.IndexAdvisor;
import com.spandigital.cel2sql.dialect.IndexPattern;
import com.spandigital.cel2sql.dialect.IndexRecommendation;
import com.spandigital.cel2sql.dialect.PatternType;
import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.dialect.SqlWriter;
import com.spandigital.cel2sql.error.ConversionException;

import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/**
 * Apache Spark SQL dialect implementation.
 *
 * <p>Ported from the Go {@code dialect/spark/dialect.go} implementation. Spark
 * runs on the JVM and uses {@code java.util.regex.Pattern}, so the regex
 * translator is mostly a passthrough. Spark has no separate JSONB type — JSON
 * fields are accessed via {@code get_json_object}; arrays use the native
 * {@code ARRAY<T>} type with {@code array_contains} / {@code size} / {@code EXPLODE}.</p>
 *
 * <p>Spark implements {@link IndexAdvisor} but always returns {@code null}: indexing
 * on Spark is storage-layer-specific (Delta Z-order vs Iceberg sort vs plain Parquet)
 * and not portable as a single set of SQL recommendations. Implementing the interface
 * (rather than omitting it) prevents {@link com.spandigital.cel2sql.Cel2Sql#analyzeQuery}
 * from silently falling back to Postgres recommendations.</p>
 */
public final class SparkDialect implements Dialect, IndexAdvisor {

    public SparkDialect() {
    }

    @Override
    public DialectName name() {
        return DialectName.SPARK;
    }

    // --- Literals ---

    @Override
    public void writeStringLiteral(StringBuilder w, String value) {
        String escaped = value.replace("'", "''");
        w.append('\'').append(escaped).append('\'');
    }

    @Override
    public void writeBytesLiteral(StringBuilder w, byte[] value) {
        w.append("X'");
        w.append(HexFormat.of().withUpperCase().formatHex(value));
        w.append('\'');
    }

    /**
     * Writes a positional placeholder ({@code ?}). Spark JDBC uses positional
     * parameters, so the index argument is unused (the converter relies on
     * parameter list order to correlate values).
     */
    @Override
    public void writeParamPlaceholder(StringBuilder w, int paramIndex) {
        w.append('?');
    }

    // --- Operators ---

    @Override
    public void writeStringConcat(StringBuilder w, SqlWriter writeLHS, SqlWriter writeRHS) throws ConversionException {
        // concat() works in all Spark versions; the || operator was added in 3.0+.
        w.append("concat(");
        writeLHS.write();
        w.append(", ");
        writeRHS.write();
        w.append(')');
    }

    @Override
    public void writeRegexMatch(StringBuilder w, SqlWriter writeTarget, String pattern, boolean caseInsensitive) throws ConversionException {
        // Spark regex uses Java pattern syntax; (?i) inline flag is honoured by the
        // engine, so caseInsensitive is folded into the pattern by SparkRegex.
        writeTarget.write();
        w.append(" RLIKE '");
        String escaped = pattern.replace("'", "''");
        w.append(escaped);
        w.append('\'');
    }

    @Override
    public void writeLikeEscape(StringBuilder w) {
        w.append(" ESCAPE '\\\\'");
    }

    @Override
    public void writeArrayMembership(StringBuilder w, SqlWriter writeElem, SqlWriter writeArray) throws ConversionException {
        w.append("array_contains(");
        writeArray.write();
        w.append(", ");
        writeElem.write();
        w.append(')');
    }

    // --- Type Casting ---

    @Override
    public void writeCastToNumeric(StringBuilder w) {
        // Spark has no postfix `::TYPE` cast; arithmetic coercion `+ 0` works
        // (same trick MySQL/SQLite use), forcing string→number coercion.
        w.append(" + 0");
    }

    @Override
    public void writeTypeName(StringBuilder w, String celTypeName) {
        switch (celTypeName) {
            case "bool" -> w.append("BOOLEAN");
            case "bytes" -> w.append("BINARY");
            case "double" -> w.append("DOUBLE");
            case "int" -> w.append("BIGINT");
            case "string" -> w.append("STRING");
            case "uint" -> w.append("BIGINT");
            default -> w.append(celTypeName.toUpperCase());
        }
    }

    @Override
    public void writeEpochExtract(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("UNIX_TIMESTAMP(");
        writeExpr.write();
        w.append(')');
    }

    @Override
    public void writeTimestampCast(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("CAST(");
        writeExpr.write();
        w.append(" AS TIMESTAMP)");
    }

    // --- Arrays ---

    @Override
    public void writeArrayLiteralOpen(StringBuilder w) {
        w.append("array(");
    }

    @Override
    public void writeArrayLiteralClose(StringBuilder w) {
        w.append(')');
    }

    @Override
    public void writeArrayLength(StringBuilder w, int dimension, SqlWriter writeExpr) throws ConversionException {
        if (dimension > 1) {
            throw ConversionException.of("Unsupported feature",
                    "Spark dialect does not support multi-dimensional array length (dimension=" + dimension + ")");
        }
        // Spark size() returns -1 for null; COALESCE collapses to 0 to match cel2sql semantics.
        w.append("COALESCE(size(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException {
        writeArray.write();
        w.append('[');
        writeIndex.write();
        w.append(']');
    }

    @Override
    public void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException {
        writeArray.write();
        w.append('[').append(index).append(']');
    }

    @Override
    public void writeEmptyTypedArray(StringBuilder w, String typeName) {
        w.append("CAST(array() AS ARRAY<").append(sparkTypeName(typeName)).append(">)");
    }

    // --- JSON ---

    @Override
    public void writeJSONFieldAccess(StringBuilder w, SqlWriter writeBase, String fieldName, boolean isFinal) throws ConversionException {
        // Spark's get_json_object always returns a string; the same function is used
        // for both intermediate and final access (Spark has no JSON_QUERY equivalent).
        w.append("get_json_object(");
        writeBase.write();
        w.append(", '$.").append(escapeJSONFieldName(fieldName)).append("')");
    }

    @Override
    public void writeJSONExistence(StringBuilder w, boolean isJSONB, String fieldName, SqlWriter writeBase) throws ConversionException {
        w.append("get_json_object(");
        writeBase.write();
        w.append(", '$.").append(escapeJSONFieldName(fieldName)).append("') IS NOT NULL");
    }

    @Override
    public void writeJSONArrayElements(StringBuilder w, boolean isJSONB, boolean asText, SqlWriter writeExpr) throws ConversionException {
        // Element type is fixed to STRING; numeric comparisons coerce via writeCastToNumeric.
        w.append("EXPLODE(from_json(");
        writeExpr.write();
        w.append(", 'ARRAY<STRING>'))");
    }

    @Override
    public void writeJSONArrayLength(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(size(from_json(");
        writeExpr.write();
        w.append(", 'ARRAY<STRING>')), 0)");
    }

    @Override
    public void writeJSONExtractPath(StringBuilder w, List<String> pathSegments, SqlWriter writeRoot) throws ConversionException {
        w.append("get_json_object(");
        writeRoot.write();
        w.append(", '$");
        for (String segment : pathSegments) {
            w.append('.').append(escapeJSONFieldName(segment));
        }
        w.append("') IS NOT NULL");
    }

    /**
     * JSON array membership ({@code elem in jsonArrayField}) is not supported on Spark.
     *
     * <p>The converter emits {@code lhs = <subquery>} for this construct, and a
     * scalar subquery built from {@code EXPLODE(from_json(...))} can return
     * multiple rows — Spark rejects that at runtime with a strict scalar-subquery
     * error. The dialect contract here does not provide the candidate element,
     * so we cannot rewrite to a boolean predicate (e.g.
     * {@code array_contains(from_json(...), elem)}). Throwing at conversion time
     * is preferable to emitting SQL that fails at execution.</p>
     *
     * <p>Workaround: when the column is typed as a native {@code ARRAY<T>}
     * (rather than a JSON string parsed via {@code from_json}), the standard
     * array-membership path ({@link #writeArrayMembership}) is used and emits
     * {@code array_contains(arr, elem)}, which works correctly.</p>
     */
    @Override
    public void writeJSONArrayMembership(StringBuilder w, String jsonFunc, SqlWriter writeExpr) throws ConversionException {
        throw ConversionException.of(
                "Unsupported operation",
                "Spark JSON array membership requires a boolean predicate (array_contains/EXISTS); "
                        + "the dialect contract does not provide the candidate element to build one. "
                        + "Use a typed ARRAY<T> column or rewrite the expression in application code.");
    }

    @Override
    public void writeNestedJSONArrayMembership(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        throw ConversionException.of(
                "Unsupported operation",
                "Spark nested JSON array membership requires a boolean predicate (array_contains/EXISTS); "
                        + "the dialect contract does not provide the candidate element to build one. "
                        + "Use a typed ARRAY<T> column or rewrite the expression in application code.");
    }

    // --- Timestamps ---

    @Override
    public void writeDuration(StringBuilder w, long value, String unit) {
        w.append("INTERVAL ").append(value).append(' ').append(unit);
    }

    @Override
    public void writeInterval(StringBuilder w, SqlWriter writeValue, String unit) throws ConversionException {
        w.append("INTERVAL ");
        writeValue.write();
        w.append(' ').append(unit);
    }

    @Override
    public void writeExtract(StringBuilder w, String part, SqlWriter writeExpr, SqlWriter writeTZ) throws ConversionException {
        // Spark dayofweek() returns 1=Sunday..7=Saturday; CEL convention is 0=Sunday..6=Saturday.
        boolean isDOW = "DOW".equals(part);
        if (isDOW) {
            w.append("(dayofweek(");
            writeExpr.write();
            if (writeTZ != null) {
                w.append(" AT TIME ZONE ");
                writeTZ.write();
            }
            w.append(") - 1)");
            return;
        }
        w.append("EXTRACT(").append(part).append(" FROM ");
        writeExpr.write();
        if (writeTZ != null) {
            w.append(" AT TIME ZONE ");
            writeTZ.write();
        }
        w.append(')');
    }

    @Override
    public void writeTimestampArithmetic(StringBuilder w, String op, SqlWriter writeTS, SqlWriter writeDur) throws ConversionException {
        writeTS.write();
        w.append(' ').append(op).append(' ');
        writeDur.write();
    }

    // --- String Functions ---

    @Override
    public void writeContains(StringBuilder w, SqlWriter writeHaystack, SqlWriter writeNeedle) throws ConversionException {
        // LOCATE(substr, str) returns 1-based position or 0 when not found.
        w.append("LOCATE(");
        writeNeedle.write();
        w.append(", ");
        writeHaystack.write();
        w.append(") > 0");
    }

    @Override
    public void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException {
        w.append("split(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(')');
    }

    @Override
    public void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException {
        // Spark 3.x+ supports the 3-arg split.
        w.append("split(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(", ").append(limit).append(')');
    }

    @Override
    public void writeJoin(StringBuilder w, SqlWriter writeArray, SqlWriter writeDelim) throws ConversionException {
        w.append("array_join(");
        writeArray.write();
        w.append(", ");
        if (writeDelim != null) {
            writeDelim.write();
        } else {
            w.append("''");
        }
        w.append(')');
    }

    @Override
    public void writeFormat(StringBuilder w, String formatSpec, java.util.List<SqlWriter> writeArgs) throws ConversionException {
        // Spark's format_string() is its printf-equivalent (supports %s/%d/%f directly).
        w.append("format_string(");
        writeStringLiteral(w, formatSpec);
        for (SqlWriter arg : writeArgs) {
            w.append(", ");
            arg.write();
        }
        w.append(')');
    }

    // --- Comprehensions ---

    @Override
    public void writeUnnest(StringBuilder w, SqlWriter writeSource) throws ConversionException {
        // The converter wraps this in subquery scaffolding. Spark uses EXPLODE for the
        // SELECT FROM UNNEST() pattern; the surrounding (SELECT collect_list(...)) wrapper
        // (see writeArraySubqueryOpen) re-collects the rows into an array.
        w.append("EXPLODE(");
        writeSource.write();
        w.append(')');
    }

    @Override
    public void writeArraySubqueryOpen(StringBuilder w) {
        // Spark has no ARRAY(SELECT ...) constructor; collect_list() is the closest equivalent.
        w.append("(SELECT collect_list(");
    }

    @Override
    public void writeArraySubqueryExprClose(StringBuilder w) {
        w.append(')');
    }

    // --- Struct ---

    @Override
    public void writeStructOpen(StringBuilder w) {
        w.append("struct(");
    }

    @Override
    public void writeStructClose(StringBuilder w) {
        w.append(')');
    }

    // --- Validation ---

    @Override
    public int maxIdentifierLength() {
        return SparkValidation.MAX_IDENTIFIER_LENGTH;
    }

    @Override
    public void validateFieldName(String name) throws ConversionException {
        SparkValidation.validateFieldName(name);
    }

    @Override
    public Set<String> reservedKeywords() {
        return SparkValidation.getReservedKeywords();
    }

    // --- Regex ---

    @Override
    public RegexResult convertRegex(String re2Pattern) throws ConversionException {
        return SparkRegex.convertRE2ToSpark(re2Pattern);
    }

    @Override
    public boolean supportsRegex() {
        return true;
    }

    // --- Capabilities ---

    @Override
    public boolean supportsNativeArrays() {
        return true;
    }

    @Override
    public boolean supportsJSONB() {
        return false;
    }

    @Override
    public boolean supportsIndexAnalysis() {
        // Spark indexing is storage-layer-specific (Delta Z-order vs Iceberg sort vs
        // plain Parquet) and not portable as a single set of SQL recommendations.
        return false;
    }

    // --- Index Advisor ---

    /**
     * Always returns {@code null}: Spark indexing is storage-layer-specific
     * (Delta, Iceberg, Parquet) and cel2sql cannot emit a single portable
     * recommendation. Callers using {@link com.spandigital.cel2sql.Cel2Sql#analyzeQuery}
     * with the Spark dialect will receive an empty recommendation list.
     */
    @Override
    public IndexRecommendation recommendIndex(IndexPattern pattern) {
        return null;
    }

    @Override
    public List<PatternType> supportedPatterns() {
        return List.of();
    }

    // --- Internal helpers ---

    private static String escapeJSONFieldName(String fieldName) {
        return fieldName.replace("'", "''");
    }

    private static String sparkTypeName(String typeName) {
        return switch (typeName.toLowerCase()) {
            case "text", "string", "varchar", "char" -> "STRING";
            case "int", "integer", "bigint", "int64", "long" -> "BIGINT";
            case "double", "float", "real", "float64" -> "DOUBLE";
            case "boolean", "bool" -> "BOOLEAN";
            case "bytes", "bytea", "blob", "binary" -> "BINARY";
            default -> typeName.toUpperCase();
        };
    }
}
