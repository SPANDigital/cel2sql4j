package com.spandigital.cel2sql.dialect.postgres;

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
 * PostgreSQL dialect implementation.
 * Implements the {@link Dialect} interface for PostgreSQL-specific SQL generation.
 *
 * <p>Ported from the Go {@code dialect/postgres/postgres.go} implementation.</p>
 */
public final class PostgresDialect implements Dialect, IndexAdvisor {

    public PostgresDialect() {
    }

    @Override
    public DialectName name() {
        return DialectName.POSTGRESQL;
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
        w.append(" ESCAPE E'\\\\'");
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
        w.append("::numeric");
    }

    @Override
    public void writeTypeName(StringBuilder w, String celTypeName) {
        switch (celTypeName) {
            case "bool" -> w.append("BOOLEAN");
            case "bytes" -> w.append("BYTEA");
            case "double" -> w.append("DOUBLE PRECISION");
            case "int" -> w.append("BIGINT");
            case "string" -> w.append("TEXT");
            case "uint" -> w.append("BIGINT");
            default -> w.append(celTypeName.toUpperCase());
        }
    }

    @Override
    public void writeEpochExtract(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("EXTRACT(EPOCH FROM ");
        writeExpr.write();
        w.append(")::bigint");
    }

    @Override
    public void writeTimestampCast(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("CAST(");
        writeExpr.write();
        w.append(" AS TIMESTAMP WITH TIME ZONE)");
    }

    // --- Arrays ---

    @Override
    public void writeArrayLiteralOpen(StringBuilder w) {
        w.append("ARRAY[");
    }

    @Override
    public void writeArrayLiteralClose(StringBuilder w) {
        w.append(']');
    }

    @Override
    public void writeArrayLength(StringBuilder w, int dimension, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(ARRAY_LENGTH(");
        writeExpr.write();
        w.append(", ").append(dimension).append("), 0)");
    }

    @Override
    public void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException {
        w.append('(');
        writeArray.write();
        w.append(")[");
        writeIndex.write();
        w.append(" + 1]");
    }

    @Override
    public void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException {
        w.append('(');
        writeArray.write();
        w.append(")[").append(index + 1).append(']');
    }

    @Override
    public void writeEmptyTypedArray(StringBuilder w, String typeName) {
        w.append("ARRAY[]::").append(typeName).append("[]");
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
        writeBase.write();
        String escapedField = escapeJSONFieldName(fieldName);
        if (isJSONB) {
            w.append(" ? '").append(escapedField).append('\'');
        } else {
            w.append("->'").append(escapedField).append("' IS NOT NULL");
        }
    }

    @Override
    public void writeJSONArrayElements(StringBuilder w, boolean isJSONB, boolean asText, SqlWriter writeExpr) throws ConversionException {
        if (isJSONB) {
            w.append(asText ? "jsonb_array_elements_text(" : "jsonb_array_elements(");
        } else {
            w.append(asText ? "json_array_elements_text(" : "json_array_elements(");
        }
        writeExpr.write();
        w.append(')');
    }

    @Override
    public void writeJSONArrayLength(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(jsonb_array_length(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeJSONExtractPath(StringBuilder w, List<String> pathSegments, SqlWriter writeRoot) throws ConversionException {
        w.append("jsonb_extract_path_text(");
        writeRoot.write();
        for (String segment : pathSegments) {
            w.append(", '").append(escapeJSONFieldName(segment)).append('\'');
        }
        w.append(") IS NOT NULL");
    }

    @Override
    public void writeJSONArrayMembership(StringBuilder w, String jsonFunc, SqlWriter writeExpr) throws ConversionException {
        w.append("ANY(ARRAY(SELECT ").append(jsonFunc).append('(');
        writeExpr.write();
        w.append(")))");
    }

    @Override
    public void writeNestedJSONArrayMembership(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("ANY(ARRAY(SELECT jsonb_array_elements_text(");
        writeExpr.write();
        w.append(")))");
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
        w.append("POSITION(");
        writeNeedle.write();
        w.append(" IN ");
        writeHaystack.write();
        w.append(") > 0");
    }

    @Override
    public void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException {
        w.append("STRING_TO_ARRAY(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append(')');
    }

    @Override
    public void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException {
        w.append("(STRING_TO_ARRAY(");
        writeStr.write();
        w.append(", ");
        writeDelim.write();
        w.append("))[1:").append(limit).append(']');
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
        w.append(", '')");
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
        // No-op for PostgreSQL
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
        return PostgresValidation.MAX_IDENTIFIER_LENGTH;
    }

    @Override
    public void validateFieldName(String name) throws ConversionException {
        PostgresValidation.validateFieldName(name);
    }

    @Override
    public Set<String> reservedKeywords() {
        return PostgresValidation.getReservedKeywords();
    }

    // --- Regex ---

    @Override
    public RegexResult convertRegex(String re2Pattern) throws ConversionException {
        return PostgresRegex.convertRE2ToPOSIX(re2Pattern);
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
        return true;
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
            case COMPARISON -> new IndexRecommendation(col, "BTREE",
                    String.format("CREATE INDEX idx_%s_btree ON %s (%s);", safeName, table, col),
                    String.format("Comparison operations on '%s' benefit from B-tree index for efficient range queries and equality checks", col));
            case JSON_ACCESS -> new IndexRecommendation(col, "GIN",
                    String.format("CREATE INDEX idx_%s_gin ON %s USING GIN (%s);", safeName, table, col),
                    String.format("JSON path operations on '%s' benefit from GIN index for efficient nested field access", col));
            case REGEX_MATCH -> new IndexRecommendation(col, "GIN",
                    String.format("CREATE INDEX idx_%s_gin_trgm ON %s USING GIN (%s gin_trgm_ops);", safeName, table, col),
                    String.format("Regex matching on '%s' benefits from GIN index with pg_trgm extension for pattern matching", col));
            case ARRAY_MEMBERSHIP -> new IndexRecommendation(col, "GIN",
                    String.format("CREATE INDEX idx_%s_gin ON %s USING GIN (%s);", safeName, table, col),
                    String.format("Array membership tests on '%s' benefit from GIN index for efficient element lookups", col));
            case ARRAY_COMPREHENSION -> new IndexRecommendation(col, "GIN",
                    String.format("CREATE INDEX idx_%s_gin ON %s USING GIN (%s);", safeName, table, col),
                    String.format("Array comprehension on '%s' benefits from GIN index for efficient array operations", col));
            case JSON_ARRAY_COMPREHENSION -> new IndexRecommendation(col, "GIN",
                    String.format("CREATE INDEX idx_%s_gin ON %s USING GIN (%s);", safeName, table, col),
                    String.format("JSONB array comprehension on '%s' benefits from GIN index for efficient array element access", col));
        };
    }

    @Override
    public List<PatternType> supportedPatterns() {
        return List.of(
                PatternType.COMPARISON, PatternType.JSON_ACCESS, PatternType.REGEX_MATCH,
                PatternType.ARRAY_MEMBERSHIP, PatternType.ARRAY_COMPREHENSION, PatternType.JSON_ARRAY_COMPREHENSION
        );
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
