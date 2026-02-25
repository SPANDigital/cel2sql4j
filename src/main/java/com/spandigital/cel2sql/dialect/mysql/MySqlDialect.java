package com.spandigital.cel2sql.dialect.mysql;

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
 * MySQL dialect implementation.
 * Implements the {@link Dialect} interface for MySQL-specific SQL generation.
 *
 * <p>Ported from the Go {@code dialect/mysql/dialect.go} implementation.</p>
 */
public final class MySqlDialect implements Dialect, IndexAdvisor {

    public MySqlDialect() {
    }

    @Override
    public DialectName name() {
        return DialectName.MYSQL;
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
        w.append("CONCAT(");
        writeLHS.write();
        w.append(", ");
        writeRHS.write();
        w.append(')');
    }

    @Override
    public void writeRegexMatch(StringBuilder w, SqlWriter writeTarget, String pattern, boolean caseInsensitive) throws ConversionException {
        writeTarget.write();
        w.append(" REGEXP ");
        String escaped = pattern.replace("'", "''");
        w.append('\'').append(escaped).append('\'');
    }

    @Override
    public void writeLikeEscape(StringBuilder w) {
        w.append(" ESCAPE '\\\\'");
    }

    @Override
    public void writeArrayMembership(StringBuilder w, SqlWriter writeElem, SqlWriter writeArray) throws ConversionException {
        w.append("JSON_CONTAINS(");
        writeArray.write();
        w.append(", CAST(");
        writeElem.write();
        w.append(" AS JSON))");
    }

    // --- Type Casting ---

    @Override
    public void writeCastToNumeric(StringBuilder w) {
        w.append(" + 0");
    }

    @Override
    public void writeTypeName(StringBuilder w, String celTypeName) {
        switch (celTypeName) {
            case "bool" -> w.append("UNSIGNED");
            case "bytes" -> w.append("BINARY");
            case "double" -> w.append("DECIMAL");
            case "int" -> w.append("SIGNED");
            case "string" -> w.append("CHAR");
            case "uint" -> w.append("UNSIGNED");
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
        w.append(" AS DATETIME)");
    }

    // --- Arrays ---

    @Override
    public void writeArrayLiteralOpen(StringBuilder w) {
        w.append("JSON_ARRAY(");
    }

    @Override
    public void writeArrayLiteralClose(StringBuilder w) {
        w.append(')');
    }

    @Override
    public void writeArrayLength(StringBuilder w, int dimension, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(JSON_LENGTH(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException {
        w.append("JSON_EXTRACT(");
        writeArray.write();
        w.append(", CONCAT('$[', ");
        writeIndex.write();
        w.append(", ']'))");
    }

    @Override
    public void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException {
        w.append("JSON_EXTRACT(");
        writeArray.write();
        w.append(", '$[").append(index).append("]')");
    }

    @Override
    public void writeEmptyTypedArray(StringBuilder w, String typeName) {
        w.append("JSON_ARRAY()");
    }

    // --- JSON ---

    @Override
    public void writeJSONFieldAccess(StringBuilder w, SqlWriter writeBase, String fieldName, boolean isFinal) throws ConversionException {
        writeBase.write();
        String escapedField = escapeJSONFieldName(fieldName);
        if (isFinal) {
            w.append("->>'$.").append(escapedField).append('\'');
        } else {
            w.append("->'$.").append(escapedField).append('\'');
        }
    }

    @Override
    public void writeJSONExistence(StringBuilder w, boolean isJSONB, String fieldName, SqlWriter writeBase) throws ConversionException {
        String escapedField = escapeJSONFieldName(fieldName);
        w.append("JSON_CONTAINS_PATH(");
        writeBase.write();
        w.append(", 'one', '$.").append(escapedField).append("')");
    }

    @Override
    public void writeJSONArrayElements(StringBuilder w, boolean isJSONB, boolean asText, SqlWriter writeExpr) throws ConversionException {
        w.append("JSON_TABLE(");
        writeExpr.write();
        w.append(", '$[*]' COLUMNS(value TEXT PATH '$'))");
    }

    @Override
    public void writeJSONArrayLength(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("COALESCE(JSON_LENGTH(");
        writeExpr.write();
        w.append("), 0)");
    }

    @Override
    public void writeJSONExtractPath(StringBuilder w, List<String> pathSegments, SqlWriter writeRoot) throws ConversionException {
        w.append("JSON_CONTAINS_PATH(");
        writeRoot.write();
        w.append(", 'one', '$");
        for (String segment : pathSegments) {
            w.append('.').append(escapeJSONFieldName(segment));
        }
        w.append("')");
    }

    @Override
    public void writeJSONArrayMembership(StringBuilder w, String jsonFunc, SqlWriter writeExpr) throws ConversionException {
        w.append("JSON_CONTAINS(");
        writeExpr.write();
        w.append(", CAST(? AS JSON))");
    }

    @Override
    public void writeNestedJSONArrayMembership(StringBuilder w, SqlWriter writeExpr) throws ConversionException {
        w.append("JSON_CONTAINS(");
        writeExpr.write();
        w.append(", CAST(? AS JSON))");
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
            w.append("(DAYOFWEEK(");
            writeExpr.write();
            if (writeTZ != null) {
                w.append(" AT TIME ZONE ");
                writeTZ.write();
            }
            w.append(") + 5) % 7");
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
        writeTS.write();
        w.append(' ').append(op).append(' ');
        writeDur.write();
    }

    // --- String Functions ---

    @Override
    public void writeContains(StringBuilder w, SqlWriter writeHaystack, SqlWriter writeNeedle) throws ConversionException {
        w.append("LOCATE(");
        writeNeedle.write();
        w.append(", ");
        writeHaystack.write();
        w.append(") > 0");
    }

    @Override
    public void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException {
        w.append("JSON_ARRAY(");
        writeStr.write();
        w.append(')');
    }

    @Override
    public void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException {
        writeSplit(w, writeStr, writeDelim);
    }

    @Override
    public void writeJoin(StringBuilder w, SqlWriter writeArray, SqlWriter writeDelim) throws ConversionException {
        w.append("JSON_UNQUOTE(");
        writeArray.write();
        w.append(')');
    }

    // --- Comprehensions ---

    @Override
    public void writeUnnest(StringBuilder w, SqlWriter writeSource) throws ConversionException {
        w.append("JSON_TABLE(");
        writeSource.write();
        w.append(", '$[*]' COLUMNS(value TEXT PATH '$'))");
    }

    @Override
    public void writeArraySubqueryOpen(StringBuilder w) {
        w.append("(SELECT JSON_ARRAYAGG(");
    }

    @Override
    public void writeArraySubqueryExprClose(StringBuilder w) {
        w.append(')');
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
        return MySqlValidation.MAX_IDENTIFIER_LENGTH;
    }

    @Override
    public void validateFieldName(String name) throws ConversionException {
        MySqlValidation.validateFieldName(name);
    }

    @Override
    public Set<String> reservedKeywords() {
        return MySqlValidation.getReservedKeywords();
    }

    // --- Regex ---

    @Override
    public RegexResult convertRegex(String re2Pattern) throws ConversionException {
        return MySqlRegex.convertRE2ToMySQL(re2Pattern);
    }

    @Override
    public boolean supportsRegex() {
        return true;
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
            case JSON_ACCESS -> new IndexRecommendation(col, "BTREE",
                    String.format("CREATE INDEX idx_%s_json ON %s ((CAST(%s->>'$.key' AS CHAR(255))));", safeName, table, col),
                    String.format("JSON field access on '%s' benefits from a functional B-tree index", col));
            case REGEX_MATCH -> new IndexRecommendation(col, "FULLTEXT",
                    String.format("CREATE FULLTEXT INDEX idx_%s_ft ON %s (%s);", safeName, table, col),
                    String.format("Regex matching on '%s' may benefit from a FULLTEXT index for pattern matching", col));
            case ARRAY_MEMBERSHIP, ARRAY_COMPREHENSION -> null;
            case JSON_ARRAY_COMPREHENSION -> new IndexRecommendation(col, "BTREE",
                    String.format("CREATE INDEX idx_%s_json ON %s ((CAST(%s->>'$.key' AS CHAR(255))));", safeName, table, col),
                    String.format("JSON array comprehension on '%s' may benefit from a functional B-tree index", col));
        };
    }

    @Override
    public List<PatternType> supportedPatterns() {
        return List.of(PatternType.COMPARISON, PatternType.JSON_ACCESS, PatternType.REGEX_MATCH, PatternType.JSON_ARRAY_COMPREHENSION);
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
