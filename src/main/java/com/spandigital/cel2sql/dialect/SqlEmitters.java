package com.spandigital.cel2sql.dialect;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Shared SQL-emission helpers for the recurring fragment shapes that several
 * dialects render identically.
 *
 * <p>Each dialect still declares its own {@code Dialect} override (so the
 * per-dialect behaviour stays explicit and greppable), but the dialects that
 * happen to share a shape delegate the actual {@link StringBuilder} writing
 * here instead of copy-pasting the body. Dialects whose output genuinely
 * differs keep their own inline implementation.</p>
 */
public final class SqlEmitters {

    private SqlEmitters() {
    }

    /**
     * Writes a two-argument function call: {@code func(a, b)}.
     */
    public static void writeBinaryCall(StringBuilder w, String func, SqlWriter writeA, SqlWriter writeB)
            throws ConversionException {
        w.append(func).append('(');
        writeA.write();
        w.append(", ");
        writeB.write();
        w.append(')');
    }

    /**
     * Writes an array-to-string join: {@code func(array, delim)} where a null
     * delimiter falls back to the empty string, optionally followed by a
     * trailing empty-string argument (PostgreSQL's {@code ARRAY_TO_STRING}
     * null-replacement parameter).
     */
    public static void writeArrayJoin(StringBuilder w, String func, SqlWriter writeArray,
                                      SqlWriter writeDelim, boolean trailingEmptyArg)
            throws ConversionException {
        w.append(func).append('(');
        writeArray.write();
        w.append(", ");
        if (writeDelim != null) {
            writeDelim.write();
        } else {
            w.append("''");
        }
        if (trailingEmptyArg) {
            w.append(", ''");
        }
        w.append(')');
    }

    /**
     * Writes the {@code json_each} membership idiom shared by SQLite and DuckDB:
     * {@code EXISTS (SELECT 1 FROM json_each(array) WHERE value = elem)}.
     */
    public static void writeJsonEachMembership(StringBuilder w, SqlWriter writeArray, SqlWriter writeElem)
            throws ConversionException {
        w.append("EXISTS (SELECT 1 FROM json_each(");
        writeArray.write();
        w.append(") WHERE value = ");
        writeElem.write();
        w.append(')');
    }

    /**
     * Writes a JSON path-existence probe: {@code func(root, '$.seg.seg...')}
     * followed by {@code suffix} (e.g. {@code " IS NOT NULL"}). Each path
     * segment is escaped via {@code escape}.
     */
    public static void writeJsonPathProbe(StringBuilder w, String func, SqlWriter writeRoot,
                                          List<String> pathSegments, String suffix, UnaryOperator<String> escape)
            throws ConversionException {
        w.append(func).append('(');
        writeRoot.write();
        w.append(", '$");
        for (String segment : pathSegments) {
            w.append('.').append(escape.apply(segment));
        }
        w.append("')").append(suffix);
    }

    /**
     * Writes an infix regex match: {@code target <op> 'pattern'} with the
     * pattern's single quotes doubled for SQL-string escaping. Used by dialects
     * whose regex operator is a binary infix token ({@code ~}/{@code ~*},
     * {@code REGEXP}, {@code RLIKE}).
     */
    public static void writeInfixRegex(StringBuilder w, SqlWriter writeTarget, String op, String pattern)
            throws ConversionException {
        writeTarget.write();
        w.append(op);
        w.append('\'').append(pattern.replace("'", "''")).append('\'');
    }

    /**
     * Writes a standard SQL {@code EXTRACT(part FROM expr [AT TIME ZONE tz])}
     * clause. The day-of-week conversion wrapping that some dialects apply is
     * left to the caller.
     */
    public static void writeStandardExtract(StringBuilder w, String part, SqlWriter writeExpr, SqlWriter writeTZ)
            throws ConversionException {
        w.append("EXTRACT(").append(part).append(" FROM ");
        writeExpr.write();
        if (writeTZ != null) {
            w.append(" AT TIME ZONE ");
            writeTZ.write();
        }
        w.append(')');
    }

    /**
     * Writes a standard {@code EXTRACT}, applying the PostgreSQL/DuckDB
     * day-of-week remapping {@code (EXTRACT(DOW FROM ...) + 6) % 7} when
     * {@code part} is {@code "DOW"}. Both engines share this exact convention.
     */
    public static void writeExtractWithPostgresDow(StringBuilder w, String part, SqlWriter writeExpr, SqlWriter writeTZ)
            throws ConversionException {
        boolean isDOW = "DOW".equals(part);
        if (isDOW) {
            w.append('(');
        }
        writeStandardExtract(w, part, writeExpr, writeTZ);
        if (isDOW) {
            w.append(" + 6) % 7");
        }
    }

    /**
     * Writes PostgreSQL/DuckDB arrow-operator JSON field access:
     * {@code base->>'field'} (final/text extraction) or {@code base->'field'}
     * (intermediate/json extraction). The field name is escaped via
     * {@code escape}.
     */
    public static void writeArrowJsonAccess(StringBuilder w, SqlWriter writeBase, String fieldName,
                                            boolean isFinal, UnaryOperator<String> escape)
            throws ConversionException {
        writeBase.write();
        w.append(isFinal ? "->>'" : "->'");
        w.append(escape.apply(fieldName)).append('\'');
    }
}
