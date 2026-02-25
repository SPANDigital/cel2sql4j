package com.spandigital.cel2sql.dialect.duckdb;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DuckDB-specific field name validation and reserved keyword management.
 * Ported from the Go {@code dialect/duckdb/validation.go} implementation.
 */
final class DuckDbValidation {

    /** DuckDB has no maximum identifier length limit. */
    static final int MAX_IDENTIFIER_LENGTH = 0;

    /** Pattern for valid DuckDB identifiers: starts with letter or underscore, then alphanumeric or underscore. */
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Set of DuckDB reserved SQL keywords (lowercased).
     * These cannot be used as unquoted identifiers.
     */
    private static final Set<String> RESERVED_SQL_KEYWORDS;

    static {
        Set<String> keywords = new HashSet<>();
        keywords.add("all");
        keywords.add("alter");
        keywords.add("analyse");
        keywords.add("analyze");
        keywords.add("and");
        keywords.add("any");
        keywords.add("array");
        keywords.add("as");
        keywords.add("asc");
        keywords.add("asymmetric");
        keywords.add("between");
        keywords.add("both");
        keywords.add("case");
        keywords.add("cast");
        keywords.add("check");
        keywords.add("collate");
        keywords.add("column");
        keywords.add("constraint");
        keywords.add("create");
        keywords.add("cross");
        keywords.add("current_catalog");
        keywords.add("current_date");
        keywords.add("current_role");
        keywords.add("current_schema");
        keywords.add("current_time");
        keywords.add("current_timestamp");
        keywords.add("current_user");
        keywords.add("default");
        keywords.add("deferrable");
        keywords.add("desc");
        keywords.add("distinct");
        keywords.add("do");
        keywords.add("else");
        keywords.add("end");
        keywords.add("except");
        keywords.add("exists");
        keywords.add("false");
        keywords.add("fetch");
        keywords.add("for");
        keywords.add("foreign");
        keywords.add("from");
        keywords.add("full");
        keywords.add("grant");
        keywords.add("group");
        keywords.add("having");
        keywords.add("in");
        keywords.add("initially");
        keywords.add("inner");
        keywords.add("intersect");
        keywords.add("into");
        keywords.add("is");
        keywords.add("isnull");
        keywords.add("join");
        keywords.add("lateral");
        keywords.add("leading");
        keywords.add("left");
        keywords.add("like");
        keywords.add("limit");
        keywords.add("localtime");
        keywords.add("localtimestamp");
        keywords.add("natural");
        keywords.add("not");
        keywords.add("notnull");
        keywords.add("null");
        keywords.add("offset");
        keywords.add("on");
        keywords.add("only");
        keywords.add("or");
        keywords.add("order");
        keywords.add("outer");
        keywords.add("overlaps");
        keywords.add("placing");
        keywords.add("primary");
        keywords.add("references");
        keywords.add("returning");
        keywords.add("right");
        keywords.add("select");
        keywords.add("session_user");
        keywords.add("similar");
        keywords.add("some");
        keywords.add("symmetric");
        keywords.add("table");
        keywords.add("then");
        keywords.add("to");
        keywords.add("trailing");
        keywords.add("true");
        keywords.add("union");
        keywords.add("unique");
        keywords.add("using");
        keywords.add("variadic");
        keywords.add("when");
        keywords.add("where");
        keywords.add("window");
        keywords.add("with");
        RESERVED_SQL_KEYWORDS = Collections.unmodifiableSet(keywords);
    }

    private DuckDbValidation() {
        // utility class
    }

    /**
     * Validates a field name for use as a DuckDB identifier.
     *
     * @param name the field name to validate
     * @throws ConversionException if the field name is empty, contains invalid characters,
     *                             or is a reserved keyword
     */
    static void validateFieldName(String name) throws ConversionException {
        if (name == null || name.isEmpty()) {
            throw new ConversionException("field name cannot be empty",
                    "field name cannot be empty");
        }
        // DuckDB has no max identifier length (MAX_IDENTIFIER_LENGTH == 0 means no limit)
        if (!FIELD_NAME_PATTERN.matcher(name).matches()) {
            String detail = String.format(
                    "field name \"%s\" must start with a letter or underscore and contain only alphanumeric characters and underscores",
                    name);
            throw new ConversionException("Invalid field name", detail);
        }
        if (RESERVED_SQL_KEYWORDS.contains(name.toLowerCase())) {
            String detail = String.format(
                    "field name \"%s\" is a reserved SQL keyword and cannot be used without quoting",
                    name);
            throw new ConversionException("Invalid field name", detail);
        }
    }

    /**
     * Returns the set of reserved SQL keywords for DuckDB.
     *
     * @return an unmodifiable set of lowercased reserved keywords
     */
    static Set<String> getReservedKeywords() {
        return RESERVED_SQL_KEYWORDS;
    }
}
