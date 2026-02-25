package com.spandigital.cel2sql.dialect.sqlite;

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
 * SQLite dialect implementation.
 * Implements the {@link Dialect} interface for SQLite-specific SQL generation.
 *
 * <p>Ported from the Go {@code dialect/sqlite/dialect.go} implementation.</p>
 */
public final class SqliteDialect implements Dialect, IndexAdvisor {

    public SqliteDialect() {
    }

    @Override
    public DialectName name() {
        return DialectName.SQLITE;
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
        w.append(HexFormat.of().formatHex(value));
        w.append('\'');
    }

    @Override
    public void writeParamPlaceholder(StringBuilder w, int paramIndex) {
        w.append('?');
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
        throw ConversionException.of("Unsupported operation", "regex matching is not supported in SQLite");
    }

    @Override
    public void writeLikeEscape(StringBuilder w) {
        w.append(" ESCAPE '\\\\'");
    }

    @Override
    public void writeArrayMembership(StringBuilder w, SqlWriter writeElem, SqlWriter writeArray) throws ConversionException {
        writeElem.write();
        w.append(" IN (SELECT value FROM json_each(");
        writeArray.write();
        w.append("))");
    }

    // --- Type Casting ---

    @Override
    public void writeCastToNumeric(StringBuilder w) {
        w.append(" + 0");
    }

    @Override
    public void writeTypeName(StringBuilder w, String celTypeName) {
        switch (celTypeName) {
            case "bool" -> w.append("INTEGER");
            case "bytes" -> w.append("BLOB");
            case "double" -> w.append("REAL");
            case "int" -> w.append("INTEGER");
            case "string" -> w.append("TEXT");
            case "uint" -> w.append("INTEGER");
            default -> w.append(celTypeName.toUpperCase());
        }
    }

    @Override
    public void writeEpochExtract(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("CAST(strftime('%s', ");
        writeExpr.write();
        w.append(") AS INTEGER)");
    }

    @Override
    public void writeTimestampCast(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("datetime(");
        writeExpr.write();
        w.append(')');
    }

    // --- Arrays ---

    @Override
    public void writeArrayLiteralOpen(StringBuilder w) {
        w.append("json_array(");
    }

    @Override
    public void writeArrayLiteralClose(StringBuilder w) {
        w.append(')');
    }

    @Override
    public void writeArrayLength(StringBuilder w, int dimension, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(json_array_length(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException {
        w.append("json_extract(");
        writeArray.write();
        w.append(", '$[' || ");
        writeIndex.write();
        w.append(" || ']')");
    }

    @Override
    public void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException {
        w.append("json_extract(");
        writeArray.write();
        w.append(", '$[").append(index).append("]')");
    }

    @Override
    public void writeEmptyTypedArray(StringBuilder w, String typeName) {
        w.append("json_array()");
    }

    // --- JSON ---

    @Override
    public void writeJSONFieldAccess(StringBuilder w, SqlWriter writeBase, String fieldName, boolean isFinal) throws ConversionException {
        String escapedField = escapeJSONFieldName(fieldName);
        w.append("json_extract(");
        writeBase.write();
        w.append(", '$.").append(escapedField).append("')");
    }

    @Override
    public void writeJSONExistence(StringBuilder w, boolean isJSONB, String fieldName, SqlWriter writeBase) throws ConversionException {
        String escapedField = escapeJSONFieldName(fieldName);
        w.append("json_type(");
        writeBase.write();
        w.append(", '$.").append(escapedField).append("') IS NOT NULL");
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
        w.append("json_type(");
        writeRoot.write();
        w.append(", '$");
        for (String segment : pathSegments) {
            w.append('.').append(escapeJSONFieldName(segment));
        }
        w.append("') IS NOT NULL");
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
        w.append(String.format("'%+d %ss'", value, unit.toLowerCase()));
    }

    @Override
    public void writeInterval(StringBuilder w, SqlWriter writeValue, String unit) throws ConversionException {
        w.append("'+'||");
        writeValue.write();
        w.append("||' ").append(unit.toLowerCase()).append("s'");
    }

    @Override
    public void writeExtract(StringBuilder w, String part, SqlWriter writeExpr, SqlWriter writeTZ) throws ConversionException {
        String format = switch (part) {
            case "YEAR" -> "%Y";
            case "MONTH" -> "%m";
            case "DAY" -> "%d";
            case "HOUR" -> "%H";
            case "MINUTE" -> "%M";
            case "SECOND" -> "%S";
            case "DOY" -> "%j";
            case "DOW" -> "%w";
            case "MILLISECONDS" -> "%f";
            default -> throw ConversionException.of("Unsupported operation",
                    "unsupported extract part: " + part);
        };
        w.append("CAST(strftime('").append(format).append("', ");
        writeExpr.write();
        w.append(") AS INTEGER)");
    }

    @Override
    public void writeTimestampArithmetic(StringBuilder w, String op, SqlWriter writeTS, SqlWriter writeDur) throws ConversionException {
        w.append("datetime(");
        writeTS.write();
        w.append(", ");
        if ("-".equals(op)) {
            w.append("'-'||");
            writeDur.write();
        } else {
            writeDur.write();
        }
        w.append(')');
    }

    // --- String Functions ---

    @Override
    public void writeContains(StringBuilder w, SqlWriter writeHaystack, SqlWriter writeNeedle) throws ConversionException {
        w.append("INSTR(");
        writeHaystack.write();
        w.append(", ");
        writeNeedle.write();
        w.append(") > 0");
    }

    @Override
    public void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException {
        throw ConversionException.of("Unsupported operation", "string split is not supported in SQLite");
    }

    @Override
    public void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException {
        throw ConversionException.of("Unsupported operation", "string split is not supported in SQLite");
    }

    @Override
    public void writeJoin(StringBuilder w, SqlWriter writeArray, SqlWriter writeDelim) throws ConversionException {
        throw ConversionException.of("Unsupported operation", "array join is not supported in SQLite");
    }

    // --- Comprehensions ---

    @Override
    public void writeUnnest(StringBuilder w, SqlWriter writeSource) throws ConversionException {
        w.append("json_each(");
        writeSource.write();
        w.append(')');
    }

    @Override
    public void writeArraySubqueryOpen(StringBuilder w) {
        w.append("(SELECT json_group_array(");
    }

    @Override
    public void writeArraySubqueryExprClose(StringBuilder w) {
        w.append(')');
    }

    // --- Struct ---

    @Override
    public void writeStructOpen(StringBuilder w) {
        w.append("json_object(");
    }

    @Override
    public void writeStructClose(StringBuilder w) {
        w.append(')');
    }

    // --- Validation ---

    @Override
    public int maxIdentifierLength() {
        return 0;
    }

    @Override
    public void validateFieldName(String name) throws ConversionException {
        SqliteValidation.validateFieldName(name);
    }

    @Override
    public Set<String> reservedKeywords() {
        return SqliteValidation.getReservedKeywords();
    }

    // --- Regex ---

    @Override
    public RegexResult convertRegex(String re2Pattern) throws ConversionException {
        throw ConversionException.of("Unsupported operation", "regex is not supported in SQLite");
    }

    @Override
    public boolean supportsRegex() {
        return false;
    }

    // --- Capabilities ---

    @Override
    public boolean supportsNativeArrays() {
        return false;
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
            case COMPARISON -> new IndexRecommendation(col, "BTREE",
                    String.format("CREATE INDEX idx_%s ON %s (%s);", safeName, table, col),
                    String.format("Comparison operations on '%s' benefit from a B-tree index for efficient range queries and equality checks", col));
            default -> null;
        };
    }

    @Override
    public List<PatternType> supportedPatterns() {
        return List.of(PatternType.COMPARISON);
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
