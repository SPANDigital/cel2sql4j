package com.spandigital.cel2sql.dialect;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.List;
import java.util.Set;

/**
 * Defines the interface for SQL dialect-specific code generation.
 * The converter calls these methods at every point where SQL syntax diverges
 * between databases. Methods receive a StringBuilder that shares the
 * converter's output buffer, and callback functions for writing sub-expressions.
 *
 * <p>This is the Java equivalent of the Go {@code dialect.Dialect} interface.</p>
 */
public interface Dialect {

    /** Returns the dialect name. */
    DialectName name();

    // --- Literals ---

    /** Writes a string literal in the dialect's syntax. */
    void writeStringLiteral(StringBuilder w, String value);

    /** Writes a byte array literal in the dialect's syntax. */
    void writeBytesLiteral(StringBuilder w, byte[] value) throws ConversionException;

    /**
     * Writes a parameter placeholder.
     * For PostgreSQL: $1, $2. For MySQL: ?, ?. For BigQuery: @p1, @p2.
     */
    void writeParamPlaceholder(StringBuilder w, int paramIndex);

    // --- Operators ---

    /** Writes a string concatenation expression. */
    void writeStringConcat(StringBuilder w, SqlWriter writeLHS, SqlWriter writeRHS) throws ConversionException;

    /** Writes a regex match expression. */
    void writeRegexMatch(StringBuilder w, SqlWriter writeTarget, String pattern, boolean caseInsensitive) throws ConversionException;

    /** Writes the LIKE escape clause. */
    void writeLikeEscape(StringBuilder w);

    /** Writes an array membership test. */
    void writeArrayMembership(StringBuilder w, SqlWriter writeElem, SqlWriter writeArray) throws ConversionException;

    // --- Type Casting ---

    /** Writes a cast to numeric type. */
    void writeCastToNumeric(StringBuilder w);

    /** Writes a type name for CAST expressions. */
    void writeTypeName(StringBuilder w, String celTypeName);

    /** Writes extraction of epoch from a timestamp. */
    void writeEpochExtract(StringBuilder w, SqlWriter writeExpr) throws ConversionException;

    /** Writes a cast to timestamp type. */
    void writeTimestampCast(StringBuilder w, SqlWriter writeExpr) throws ConversionException;

    // --- Arrays ---

    /** Writes the opening of an array literal. */
    void writeArrayLiteralOpen(StringBuilder w);

    /** Writes the closing of an array literal. */
    void writeArrayLiteralClose(StringBuilder w);

    /** Writes an array length expression. */
    void writeArrayLength(StringBuilder w, int dimension, SqlWriter writeExpr) throws ConversionException;

    /** Writes a list index expression with dynamic index. */
    void writeListIndex(StringBuilder w, SqlWriter writeArray, SqlWriter writeIndex) throws ConversionException;

    /** Writes a constant list index (converts 0-indexed to dialect-appropriate). */
    void writeListIndexConst(StringBuilder w, SqlWriter writeArray, long index) throws ConversionException;

    /** Writes an empty typed array literal. */
    void writeEmptyTypedArray(StringBuilder w, String typeName);

    // --- JSON ---

    /** Writes JSON field access. */
    void writeJSONFieldAccess(StringBuilder w, SqlWriter writeBase, String fieldName, boolean isFinal) throws ConversionException;

    /** Writes a JSON key existence check. */
    void writeJSONExistence(StringBuilder w, boolean isJSONB, String fieldName, SqlWriter writeBase) throws ConversionException;

    /** Writes a call to extract JSON array elements. */
    void writeJSONArrayElements(StringBuilder w, boolean isJSONB, boolean asText, SqlWriter writeExpr) throws ConversionException;

    /** Writes a JSON array length expression. */
    void writeJSONArrayLength(StringBuilder w, SqlWriter writeExpr) throws ConversionException;

    /** Writes a JSON path extraction function. */
    void writeJSONExtractPath(StringBuilder w, List<String> pathSegments, SqlWriter writeRoot) throws ConversionException;

    /** Writes a JSON array membership test for the IN operator. */
    void writeJSONArrayMembership(StringBuilder w, String jsonFunc, SqlWriter writeExpr) throws ConversionException;

    /** Writes a nested JSON array membership test. */
    void writeNestedJSONArrayMembership(StringBuilder w, SqlWriter writeExpr) throws ConversionException;

    // --- Timestamps ---

    /** Writes a duration/interval literal. */
    void writeDuration(StringBuilder w, long value, String unit);

    /** Writes an INTERVAL expression from a variable. */
    void writeInterval(StringBuilder w, SqlWriter writeValue, String unit) throws ConversionException;

    /** Writes a timestamp field extraction expression. */
    void writeExtract(StringBuilder w, String part, SqlWriter writeExpr, SqlWriter writeTZ) throws ConversionException;

    /** Writes timestamp arithmetic. */
    void writeTimestampArithmetic(StringBuilder w, String op, SqlWriter writeTS, SqlWriter writeDur) throws ConversionException;

    // --- String Functions ---

    /** Writes a string contains expression. */
    void writeContains(StringBuilder w, SqlWriter writeHaystack, SqlWriter writeNeedle) throws ConversionException;

    /** Writes a string split expression. */
    void writeSplit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim) throws ConversionException;

    /** Writes a string split expression with a limit. */
    void writeSplitWithLimit(StringBuilder w, SqlWriter writeStr, SqlWriter writeDelim, long limit) throws ConversionException;

    /** Writes an array join expression. */
    void writeJoin(StringBuilder w, SqlWriter writeArray, SqlWriter writeDelim) throws ConversionException;

    // --- Comprehensions ---

    /** Writes the UNNEST source for comprehensions. */
    void writeUnnest(StringBuilder w, SqlWriter writeSource) throws ConversionException;

    /** Writes the prefix before the transform expression in an array-building subquery. */
    void writeArraySubqueryOpen(StringBuilder w);

    /** Writes the suffix after the transform expression and before FROM. */
    void writeArraySubqueryExprClose(StringBuilder w);

    // --- Struct ---

    /** Writes the opening of a struct/row literal. */
    void writeStructOpen(StringBuilder w);

    /** Writes the closing of a struct/row literal. */
    void writeStructClose(StringBuilder w);

    // --- Validation ---

    /** Returns the maximum identifier length for this dialect. */
    int maxIdentifierLength();

    /** Validates a field name for this dialect. */
    void validateFieldName(String name) throws ConversionException;

    /** Returns the set of reserved SQL keywords for this dialect. */
    Set<String> reservedKeywords();

    // --- Regex ---

    /**
     * Converts an RE2 regex pattern to the dialect's native format.
     *
     * @return a {@link RegexResult} containing the converted pattern and case sensitivity flag
     */
    RegexResult convertRegex(String re2Pattern) throws ConversionException;

    /** Indicates whether this dialect supports regex matching. */
    boolean supportsRegex();

    // --- Capabilities ---

    /** Indicates whether this dialect has native array types. */
    boolean supportsNativeArrays();

    /** Indicates whether this dialect has a distinct JSONB type. */
    boolean supportsJSONB();

    /** Indicates whether index analysis is supported. */
    boolean supportsIndexAnalysis();
}
