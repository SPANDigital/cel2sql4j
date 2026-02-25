package com.spandigital.cel2sql.dialect.bigquery;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * BigQuery-specific field name validation and reserved keyword management.
 * Ported from the Go {@code dialect/bigquery/validation.go} implementation.
 */
final class BigQueryValidation {

    /** Maximum identifier length in BigQuery. */
    static final int MAX_IDENTIFIER_LENGTH = 300;

    /** Pattern for valid BigQuery identifiers: starts with letter or underscore, then alphanumeric or underscore. */
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Set of BigQuery reserved SQL keywords (lowercased).
     * These cannot be used as unquoted identifiers.
     */
    private static final Set<String> RESERVED_SQL_KEYWORDS;

    static {
        Set<String> keywords = new HashSet<>();
        keywords.add("all");
        keywords.add("and");
        keywords.add("any");
        keywords.add("array");
        keywords.add("as");
        keywords.add("asc");
        keywords.add("assert_rows_modified");
        keywords.add("at");
        keywords.add("between");
        keywords.add("by");
        keywords.add("case");
        keywords.add("cast");
        keywords.add("collate");
        keywords.add("contains");
        keywords.add("create");
        keywords.add("cross");
        keywords.add("cube");
        keywords.add("current");
        keywords.add("default");
        keywords.add("define");
        keywords.add("desc");
        keywords.add("distinct");
        keywords.add("else");
        keywords.add("end");
        keywords.add("enum");
        keywords.add("escape");
        keywords.add("except");
        keywords.add("exclude");
        keywords.add("exists");
        keywords.add("extract");
        keywords.add("false");
        keywords.add("fetch");
        keywords.add("following");
        keywords.add("for");
        keywords.add("from");
        keywords.add("full");
        keywords.add("group");
        keywords.add("grouping");
        keywords.add("groups");
        keywords.add("hash");
        keywords.add("having");
        keywords.add("if");
        keywords.add("ignore");
        keywords.add("in");
        keywords.add("inner");
        keywords.add("intersect");
        keywords.add("interval");
        keywords.add("into");
        keywords.add("is");
        keywords.add("join");
        keywords.add("lateral");
        keywords.add("left");
        keywords.add("like");
        keywords.add("limit");
        keywords.add("lookup");
        keywords.add("merge");
        keywords.add("natural");
        keywords.add("new");
        keywords.add("no");
        keywords.add("not");
        keywords.add("null");
        keywords.add("nulls");
        keywords.add("of");
        keywords.add("on");
        keywords.add("or");
        keywords.add("order");
        keywords.add("outer");
        keywords.add("over");
        keywords.add("partition");
        keywords.add("preceding");
        keywords.add("proto");
        keywords.add("range");
        keywords.add("recursive");
        keywords.add("respect");
        keywords.add("right");
        keywords.add("rollup");
        keywords.add("rows");
        keywords.add("select");
        keywords.add("set");
        keywords.add("some");
        keywords.add("struct");
        keywords.add("tablesample");
        keywords.add("then");
        keywords.add("to");
        keywords.add("treat");
        keywords.add("true");
        keywords.add("unbounded");
        keywords.add("union");
        keywords.add("unnest");
        keywords.add("using");
        keywords.add("when");
        keywords.add("where");
        keywords.add("window");
        keywords.add("with");
        keywords.add("within");
        RESERVED_SQL_KEYWORDS = Collections.unmodifiableSet(keywords);
    }

    private BigQueryValidation() {
        // utility class
    }

    /**
     * Validates a field name for use as a BigQuery identifier.
     *
     * @param name the field name to validate
     * @throws ConversionException if the field name is empty, too long, contains invalid characters,
     *                             or is a reserved keyword
     */
    static void validateFieldName(String name) throws ConversionException {
        if (name == null || name.isEmpty()) {
            throw new ConversionException("field name cannot be empty",
                    "field name cannot be empty");
        }
        if (name.length() > MAX_IDENTIFIER_LENGTH) {
            String detail = String.format(
                    "field name \"%s\" exceeds BigQuery maximum identifier length of %d characters",
                    name, MAX_IDENTIFIER_LENGTH);
            throw new ConversionException("Invalid field name", detail);
        }
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
     * Returns the set of reserved SQL keywords for BigQuery.
     *
     * @return an unmodifiable set of lowercased reserved keywords
     */
    static Set<String> getReservedKeywords() {
        return RESERVED_SQL_KEYWORDS;
    }
}
