package com.spandigital.cel2sql.dialect.bigquery;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.DialectName;
import com.spandigital.cel2sql.dialect.IndexAdvisor;
import com.spandigital.cel2sql.dialect.IndexPattern;
import com.spandigital.cel2sql.dialect.IndexRecommendation;
import com.spandigital.cel2sql.dialect.PatternType;
import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.dialect.SqlWriter;
import com.spandigital.cel2sql.error.ConversionException;

import java.util.List;
import java.util.Set;

/**
 * BigQuery dialect implementation.
 * Implements the {@link Dialect} interface for BigQuery-specific SQL generation.
 *
 * <p>Ported from the Go {@code dialect/bigquery/dialect.go} implementation.</p>
 */
public final class BigQueryDialect implements Dialect, IndexAdvisor {

    public BigQueryDialect() {
    }

    @Override
    public DialectName name() {
        return DialectName.BIGQUERY;
    }

    // --- Literals ---

    @Override
    public void writeStringLiteral(StringBuilder w, String value) {
        String escaped = value.replace("'", "\\'");
        w.append('\'').append(escaped).append('\'');
    }

    @Override
    public void writeBytesLiteral(StringBuilder w, byte[] value) {
        w.append("b\"");
        for (byte b : value) {
            w.append(String.format("\\%03o", b & 0xFF));
        }
        w.append('"');
    }

    @Override
    public void writeParamPlaceholder(StringBuilder w, int paramIndex) {
        w.append("@p").append(paramIndex);
    }

    // --- Operators ---

    @Override
    public void writeStringConcat(StringBuilder w, SqlWriter writeLHS, SqlWriter writeRHS) throws ConversionException {
        writeLHS.write();
        w.append(" || ");
        writeRHS.write();
    }

    @Override
    public void writeRegexMatch(StringBuilder w, SqlWriter writeTarget, String pattern, boolean caseInsensitive) throws ConversionException {
        w.append("REGEXP_CONTAINS(");
        writeTarget.write();
        w.append(", '");
        String escaped = pattern.replace("'", "\\'");
        w.append(escaped);
        w.append("')");
    }

    @Override
    public void writeLikeEscape(StringBuilder w) {
        // No-op: BigQuery uses backslash as the default escape character, no ESCAPE keyword needed
    }

    @Override
    public void writeArrayMembership(StringBuilder w, SqlWriter writeElem, SqlWriter writeArray) throws ConversionException {
        writeElem.write();
        w.append(" IN UNNEST(");
        writeArray.write();
        w.append(')');
    }

    // --- Type Casting ---

    @Override
    public void writeCastToNumeric(StringBuilder w) {
        w.append("::FLOAT64");
    }

    @Override
    public void writeTypeName(StringBuilder w, String celTypeName) {
        switch (celTypeName) {
            case "bool" -> w.append("BOOL");
            case "bytes" -> w.append("BYTES");
            case "double" -> w.append("FLOAT64");
            case "int" -> w.append("INT64");
            case "string" -> w.append("STRING");
            case "uint" -> w.append("INT64");
            default -> w.append(celTypeName.toUpperCase());
        }
    }

    @Override
    public void writeEpochExtract(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("UNIX_SECONDS(");
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
        w.append('[');
    }

    @Override
    public void writeArrayLiteralClose(StringBuilder w) {
        w.append(']');
    }

    @Override
    public void writeArrayLength(StringBuilder w, int dimension, SqlWriter writeExpr) throws ConversionException {
        w.append("ARRAY_LENGTH(");
        writeExpr.write();
        w.append(')');
    }

    @Override
    public void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException {
        writeArray.write();
        w.append("[OFFSET(");
        writeIndex.write();
        w.append(")]");
    }

    @Override
    public void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException {
        writeArray.write();
        w.append("[OFFSET(").append(index).append(")]");
    }

    @Override
    public void writeEmptyTypedArray(StringBuilder w, String typeName) {
        w.append("ARRAY<").append(bigqueryTypeName(typeName)).append(">[]");
    }

    // --- JSON ---

    @Override
    public void writeJSONFieldAccess(StringBuilder w, SqlWriter writeBase, String fieldName, boolean isFinal) throws ConversionException {
        String escapedField = escapeJSONFieldName(fieldName);
        if (isFinal) {
            w.append("JSON_VALUE(");
        } else {
            w.append("JSON_QUERY(");
        }
        writeBase.write();
        w.append(", '$.").append(escapedField).append("')");
    }

    @Override
    public void writeJSONExistence(StringBuilder w, boolean isJSONB, String fieldName, SqlWriter writeBase) throws ConversionException {
        String escapedField = escapeJSONFieldName(fieldName);
        w.append("JSON_VALUE(");
        writeBase.write();
        w.append(", '$.").append(escapedField).append("') IS NOT NULL");
    }

    @Override
    public void writeJSONArrayElements(StringBuilder w, boolean isJSONB, boolean asText, SqlWriter writeExpr) throws ConversionException {
        w.append("UNNEST(JSON_QUERY_ARRAY(");
        writeExpr.write();
        w.append("))");
    }

    @Override
    public void writeJSONArrayLength(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("ARRAY_LENGTH(JSON_QUERY_ARRAY(");
        writeExpr.write();
        w.append("))");
    }

    @Override
    public void writeJSONExtractPath(StringBuilder w, List<String> pathSegments, SqlWriter writeRoot) throws ConversionException {
        w.append("JSON_VALUE(");
        writeRoot.write();
        w.append(", '$");
        for (String segment : pathSegments) {
            w.append('.').append(escapeJSONFieldName(segment));
        }
        w.append("') IS NOT NULL");
    }

    @Override
    public void writeJSONArrayMembership(StringBuilder w, String jsonFunc, SqlWriter writeExpr) throws ConversionException {
        w.append("UNNEST(JSON_VALUE_ARRAY(");
        writeExpr.write();
        w.append("))");
    }

    @Override
    public void writeNestedJSONArrayMembership(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("UNNEST(JSON_VALUE_ARRAY(");
        writeExpr.write();
        w.append("))");
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
        boolean isDOW = "DOW".equals(part);
        if (isDOW) {
            w.append("(EXTRACT(DAYOFWEEK FROM ");
            writeExpr.write();
            if (writeTZ != null) {
                w.append(" AT TIME ZONE ");
                writeTZ.write();
            }
            w.append(") - 1)");
        } else {
            w.append("EXTRACT(").append(part).append(" FROM ");
            writeExpr.write();
            if (writeTZ != null) {
                w.append(" AT TIME ZONE ");
                writeTZ.write();
            }
            w.append(')');
        }
    }

    @Override
    public void writeTimestampArithmetic(StringBuilder w, String op, SqlWriter writeTS, SqlWriter writeDur) throws ConversionException {
        if ("+".equals(op)) {
            w.append("TIMESTAMP_ADD(");
        } else {
            w.append("TIMESTAMP_SUB(");
        }
        writeTS.write();
        w.append(", ");
        writeDur.write();
        w.append(')');
    }

    // --- String Functions ---

    @Override
    public void writeContains(StringBuilder w, SqlWriter writeHaystack, SqlWriter writeNeedle) throws ConversionException {
        w.append("STRPOS(");
        writeHaystack.write();
        w.append(", ");
        writeNeedle.write();
        w.append(") > 0");
    }

    @Override
    public void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException {
        w.append("SPLIT(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(')');
    }

    @Override
    public void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException {
        w.append("ARRAY(SELECT x FROM UNNEST(SPLIT(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(")) AS x WITH OFFSET WHERE OFFSET < ").append(limit).append(')');
    }

    @Override
    public void writeJoin(StringBuilder w, SqlWriter writeArray, SqlWriter writeDelim) throws ConversionException {
        w.append("ARRAY_TO_STRING(");
        writeArray.write();
        w.append(", ");
        if (writeDelim != null) {
            writeDelim.write();
        } else {
            w.append("''");
        }
        w.append(')');
    }

    // --- Comprehensions ---

    @Override
    public void writeUnnest(StringBuilder w, SqlWriter writeSource) throws ConversionException {
        w.append("UNNEST(");
        writeSource.write();
        w.append(')');
    }

    @Override
    public void writeArraySubqueryOpen(StringBuilder w) {
        w.append("ARRAY(SELECT ");
    }

    @Override
    public void writeArraySubqueryExprClose(StringBuilder w) {
        // No-op for BigQuery
    }

    // --- Struct ---

    @Override
    public void writeStructOpen(StringBuilder w) {
        w.append("STRUCT(");
    }

    @Override
    public void writeStructClose(StringBuilder w) {
        w.append(')');
    }

    // --- Validation ---

    @Override
    public int maxIdentifierLength() {
        return BigQueryValidation.MAX_IDENTIFIER_LENGTH;
    }

    @Override
    public void validateFieldName(String name) throws ConversionException {
        BigQueryValidation.validateFieldName(name);
    }

    @Override
    public Set<String> reservedKeywords() {
        return BigQueryValidation.getReservedKeywords();
    }

    // --- Regex ---

    @Override
    public RegexResult convertRegex(String re2Pattern) throws ConversionException {
        return BigQueryRegex.convertRE2ToBigQuery(re2Pattern);
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
        return true;
    }

    // --- Index Advisor ---

    @Override
    public IndexRecommendation recommendIndex(IndexPattern pattern) {
        String table = pattern.tableHint() != null && !pattern.tableHint().isEmpty() ? pattern.tableHint() : "table_name";
        String col = pattern.column();
        String safeName = sanitizeIndexName(col);

        return switch (pattern.pattern()) {
            case COMPARISON -> new IndexRecommendation(col, "CLUSTERING",
                    String.format("-- Add '%s' as a clustering key on %s", col, table),
                    String.format("Comparison operations on '%s' benefit from clustering for efficient range queries", col));
            case JSON_ACCESS -> new IndexRecommendation(col, "SEARCH_INDEX",
                    String.format("CREATE SEARCH INDEX idx_%s ON %s (%s);", safeName, table, col),
                    String.format("JSON field access on '%s' benefits from a search index", col));
            case REGEX_MATCH -> null;
            case ARRAY_MEMBERSHIP, ARRAY_COMPREHENSION -> null;
            case JSON_ARRAY_COMPREHENSION -> new IndexRecommendation(col, "SEARCH_INDEX",
                    String.format("CREATE SEARCH INDEX idx_%s ON %s (%s);", safeName, table, col),
                    String.format("JSON array comprehension on '%s' benefits from a search index", col));
        };
    }

    @Override
    public List<PatternType> supportedPatterns() {
        return List.of(PatternType.COMPARISON, PatternType.JSON_ACCESS, PatternType.JSON_ARRAY_COMPREHENSION);
    }

    // --- Internal helpers ---

    private static String escapeJSONFieldName(String fieldName) {
        return fieldName.replace("'", "\\'");
    }

    private static String sanitizeIndexName(String column) {
        String sanitized = column.replace(".", "_").replace(" ", "_").replace("-", "_");
        return sanitized.length() > 50 ? sanitized.substring(0, 50) : sanitized;
    }

    private static String bigqueryTypeName(String typeName) {
        return switch (typeName.toLowerCase()) {
            case "text", "string", "varchar" -> "STRING";
            case "int", "integer", "bigint", "int64" -> "INT64";
            case "double", "float", "real", "float64" -> "FLOAT64";
            case "boolean", "bool" -> "BOOL";
            case "bytes", "bytea", "blob" -> "BYTES";
            default -> typeName.toUpperCase();
        };
    }
}
