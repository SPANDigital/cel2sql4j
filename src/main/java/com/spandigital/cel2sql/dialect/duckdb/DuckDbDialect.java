package com.spandigital.cel2sql.dialect.duckdb;

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
 * DuckDB dialect implementation.
 * Implements the {@link Dialect} interface for DuckDB-specific SQL generation.
 *
 * <p>Ported from the Go {@code dialect/duckdb/dialect.go} implementation.</p>
 */
public final class DuckDbDialect implements Dialect, IndexAdvisor {

    public DuckDbDialect() {
    }

    @Override
    public DialectName name() {
        return DialectName.DUCKDB;
    }

    // --- Literals ---

    @Override
    public void writeStringLiteral(StringBuilder w, String value) {
        String escaped = value.replace("'", "''");
        w.append('\'').append(escaped).append('\'');
    }

    @Override
    public void writeBytesLiteral(StringBuilder w, byte[] value) {
        w.append("'\\x");
        w.append(HexFormat.of().formatHex(value));
        w.append('\'');
    }

    @Override
    public void writeParamPlaceholder(StringBuilder w, int paramIndex) {
        w.append('$').append(paramIndex);
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
        writeTarget.write();
        if (caseInsensitive) {
            w.append(" ~* ");
        } else {
            w.append(" ~ ");
        }
        String escaped = pattern.replace("'", "''");
        w.append('\'').append(escaped).append('\'');
    }

    @Override
    public void writeLikeEscape(StringBuilder w) {
        w.append(" ESCAPE '\\'");
    }

    @Override
    public void writeArrayMembership(StringBuilder w, SqlWriter writeElem, SqlWriter writeArray) throws ConversionException {
        writeElem.write();
        w.append(" = ANY(");
        writeArray.write();
        w.append(')');
    }

    // --- Type Casting ---

    @Override
    public void writeCastToNumeric(StringBuilder w) {
        w.append("::DOUBLE");
    }

    @Override
    public void writeTypeName(StringBuilder w, String celTypeName) {
        switch (celTypeName) {
            case "bool" -> w.append("BOOLEAN");
            case "bytes" -> w.append("BLOB");
            case "double" -> w.append("DOUBLE");
            case "int" -> w.append("BIGINT");
            case "string" -> w.append("VARCHAR");
            case "uint" -> w.append("UBIGINT");
            default -> w.append(celTypeName.toUpperCase());
        }
    }

    @Override
    public void writeEpochExtract(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("EXTRACT(EPOCH FROM ");
        writeExpr.write();
        w.append(")::BIGINT");
    }

    @Override
    public void writeTimestampCast(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("CAST(");
        writeExpr.write();
        w.append(" AS TIMESTAMPTZ)");
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
        w.append("COALESCE(array_length(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException {
        writeArray.write();
        w.append('[');
        writeIndex.write();
        w.append(" + 1]");
    }

    @Override
    public void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException {
        writeArray.write();
        w.append('[').append(index + 1).append(']');
    }

    @Override
    public void writeEmptyTypedArray(StringBuilder w, String typeName) {
        w.append("[]::").append(typeName).append("[]");
    }

    // --- JSON ---

    @Override
    public void writeJSONFieldAccess(StringBuilder w, SqlWriter writeBase, String fieldName, boolean isFinal) throws ConversionException {
        writeBase.write();
        String escapedField = escapeJSONFieldName(fieldName);
        if (isFinal) {
            w.append("->>'");
        } else {
            w.append("->'");
        }
        w.append(escapedField).append('\'');
    }

    @Override
    public void writeJSONExistence(StringBuilder w, boolean isJSONB, String fieldName, SqlWriter writeBase) throws ConversionException {
        w.append("json_exists(");
        writeBase.write();
        String escapedField = escapeJSONFieldName(fieldName);
        w.append(", '$.").append(escapedField).append("')");
    }

    @Override
    public void writeJSONArrayElements(StringBuilder w, boolean isJSONB, boolean asText, SqlWriter writeExpr) throws ConversionException {
        w.append("json_each(");
        writeExpr.write();
        w.append(')');
    }

    @Override
    public void writeJSONArrayLength(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(json_array_length(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeJSONExtractPath(StringBuilder w, List<String> pathSegments, SqlWriter writeRoot) throws ConversionException {
        w.append("json_exists(");
        writeRoot.write();
        w.append(", '$");
        for (String segment : pathSegments) {
            w.append('.').append(escapeJSONFieldName(segment));
        }
        w.append("')");
    }

    @Override
    public void writeJSONArrayMembership(StringBuilder w, String jsonFunc, SqlWriter writeExpr) throws ConversionException {
        w.append("(SELECT value FROM json_each(");
        writeExpr.write();
        w.append("))");
    }

    @Override
    public void writeNestedJSONArrayMembership(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("(SELECT value FROM json_each(");
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
            w.append('(');
        }
        w.append("EXTRACT(").append(part).append(" FROM ");
        writeExpr.write();
        if (writeTZ != null) {
            w.append(" AT TIME ZONE ");
            writeTZ.write();
        }
        w.append(')');
        if (isDOW) {
            w.append(" + 6) % 7");
        }
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
        w.append("CONTAINS(");
        writeHaystack.write();
        w.append(", ");
        writeNeedle.write();
        w.append(')');
    }

    @Override
    public void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException {
        w.append("STRING_SPLIT(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(')');
    }

    @Override
    public void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException {
        w.append("STRING_SPLIT(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(")[1:").append(limit).append(']');
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
        // No-op for DuckDB
    }

    // --- Struct ---

    @Override
    public void writeStructOpen(StringBuilder w) {
        w.append("ROW(");
    }

    @Override
    public void writeStructClose(StringBuilder w) {
        w.append(')');
    }

    // --- Validation ---

    @Override
    public int maxIdentifierLength() {
        return DuckDbValidation.MAX_IDENTIFIER_LENGTH;
    }

    @Override
    public void validateFieldName(String name) throws ConversionException {
        DuckDbValidation.validateFieldName(name);
    }

    @Override
    public Set<String> reservedKeywords() {
        return DuckDbValidation.getReservedKeywords();
    }

    // --- Regex ---

    @Override
    public RegexResult convertRegex(String re2Pattern) throws ConversionException {
        return DuckDbRegex.convertRE2ToDuckDB(re2Pattern);
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
            case COMPARISON -> new IndexRecommendation(col, "ART",
                    String.format("CREATE INDEX idx_%s ON %s (%s);", safeName, table, col),
                    String.format("Comparison operations on '%s' benefit from an ART index for efficient range queries and equality checks", col));
            case JSON_ACCESS -> new IndexRecommendation(col, "ART",
                    String.format("CREATE INDEX idx_%s_json ON %s (%s);", safeName, table, col),
                    String.format("JSON field access on '%s' may benefit from an ART index", col));
            case REGEX_MATCH -> null;
            case ARRAY_MEMBERSHIP, ARRAY_COMPREHENSION -> new IndexRecommendation(col, "ART",
                    String.format("CREATE INDEX idx_%s ON %s (%s);", safeName, table, col),
                    String.format("Array operations on '%s' may benefit from an ART index", col));
            case JSON_ARRAY_COMPREHENSION -> new IndexRecommendation(col, "ART",
                    String.format("CREATE INDEX idx_%s_json ON %s (%s);", safeName, table, col),
                    String.format("JSON array comprehension on '%s' may benefit from an ART index", col));
        };
    }

    @Override
    public List<PatternType> supportedPatterns() {
        return List.of(PatternType.COMPARISON, PatternType.JSON_ACCESS, PatternType.ARRAY_MEMBERSHIP,
                PatternType.ARRAY_COMPREHENSION, PatternType.JSON_ARRAY_COMPREHENSION);
    }

    // --- Internal helpers ---

    private static String escapeJSONFieldName(String fieldName) {
        return fieldName.replace("'", "''");
    }

    private static String sanitizeIndexName(String column) {
        String sanitized = column.replace(".", "_").replace(" ", "_").replace("-", "_");
        return sanitized.length() > 50 ? sanitized.substring(0, 50) : sanitized;
    }
}
