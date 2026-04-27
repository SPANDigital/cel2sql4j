package com.spandigital.cel2sql.dialect.spark;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Spark-specific field name validation and reserved keyword management.
 * Ported from the Go {@code dialect/spark/validation.go} implementation.
 */
final class SparkValidation {

    /** Spark / Hive identifier limit. */
    static final int MAX_IDENTIFIER_LENGTH = 128;

    /** Pattern for valid Spark identifiers (unquoted form): letter or underscore start, alphanumeric or underscore body. */
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Spark SQL reserved keywords (lowercased). Sourced from the Apache Spark docs
     * (sql-ref-ansi-compliance.html#sql-keywords) plus the standard SQL set.
     */
    private static final Set<String> RESERVED_SQL_KEYWORDS;

    static {
        Set<String> kw = new HashSet<>();
        Collections.addAll(kw,
                "all", "alter", "and", "anti", "any", "array", "as", "asc", "between", "both",
                "by", "case", "cast", "check", "cluster", "collate", "column", "create", "cross",
                "cube", "current", "current_date", "current_time", "current_timestamp",
                "current_user", "default", "delete", "desc", "describe", "distinct", "drop",
                "else", "end", "escape", "except", "exists", "false", "fetch", "filter", "for",
                "foreign", "from", "full", "function", "grant", "group", "grouping", "having",
                "hour", "in", "inner", "insert", "intersect", "interval", "into", "is", "join",
                "lateral", "leading", "left", "like", "limit", "local", "map", "minute", "month",
                "natural", "no", "not", "null", "of", "on", "only", "or", "order", "outer",
                "overlaps", "primary", "references", "right", "rollup", "row", "rows", "second",
                "select", "semi", "session_user", "set", "some", "struct", "table", "tablesample",
                "then", "time", "to", "trailing", "true", "union", "unique", "unknown", "update",
                "user", "using", "values", "when", "where", "window", "with", "year");
        RESERVED_SQL_KEYWORDS = Collections.unmodifiableSet(kw);
    }

    private SparkValidation() {}

    static void validateFieldName(String name) throws ConversionException {
        if (name == null || name.isEmpty()) {
            throw new ConversionException(
                    "Invalid field name",
                    "field name cannot be empty");
        }
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            throw new ConversionException(
                    "Invalid field name",
                    String.format("field name length %d exceeds Spark limit of %d",
                            name.length(), MAX_IDENTIFIER_LENGTH));
        }
        if (!FIELD_NAME_PATTERN.matcher(name).matches()) {
            throw new ConversionException(
                    "Invalid field name",
                    String.format("field name '%s' must start with a letter or underscore "
                            + "and contain only alphanumeric characters and underscores", name));
        }
        if (RESERVED_SQL_KEYWORDS.contains(name.toLowerCase())) {
            throw new ConversionException(
                    "Invalid field name",
                    String.format("field name '%s' is a reserved SQL keyword and cannot be used "
                            + "without quoting", name));
        }
    }

    static Set<String> getReservedKeywords() {
        return RESERVED_SQL_KEYWORDS;
    }
}
