package com.spandigital.cel2sql.dialect.sqlite;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * SQLite-specific field name validation and reserved keyword management.
 * Ported from the Go {@code dialect/sqlite/validation.go} implementation.
 */
final class SqliteValidation {

    /** Pattern for valid SQLite identifiers: starts with letter or underscore, then alphanumeric or underscore. */
    private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    /**
     * Set of SQLite reserved SQL keywords (lowercased).
     * These cannot be used as unquoted identifiers.
     */
    private static final Set<String> RESERVED_SQL_KEYWORDS;

    static {
        Set<String> keywords = new HashSet<>();
        keywords.add("abort");
        keywords.add("action");
        keywords.add("add");
        keywords.add("after");
        keywords.add("all");
        keywords.add("alter");
        keywords.add("always");
        keywords.add("analyze");
        keywords.add("and");
        keywords.add("as");
        keywords.add("asc");
        keywords.add("attach");
        keywords.add("autoincrement");
        keywords.add("before");
        keywords.add("begin");
        keywords.add("between");
        keywords.add("by");
        keywords.add("cascade");
        keywords.add("case");
        keywords.add("cast");
        keywords.add("check");
        keywords.add("collate");
        keywords.add("column");
        keywords.add("commit");
        keywords.add("conflict");
        keywords.add("constraint");
        keywords.add("create");
        keywords.add("cross");
        keywords.add("current");
        keywords.add("current_date");
        keywords.add("current_time");
        keywords.add("current_timestamp");
        keywords.add("database");
        keywords.add("default");
        keywords.add("deferrable");
        keywords.add("deferred");
        keywords.add("delete");
        keywords.add("desc");
        keywords.add("detach");
        keywords.add("distinct");
        keywords.add("do");
        keywords.add("drop");
        keywords.add("each");
        keywords.add("else");
        keywords.add("end");
        keywords.add("escape");
        keywords.add("except");
        keywords.add("exclude");
        keywords.add("exclusive");
        keywords.add("exists");
        keywords.add("explain");
        keywords.add("fail");
        keywords.add("filter");
        keywords.add("first");
        keywords.add("following");
        keywords.add("for");
        keywords.add("foreign");
        keywords.add("from");
        keywords.add("full");
        keywords.add("glob");
        keywords.add("group");
        keywords.add("groups");
        keywords.add("having");
        keywords.add("if");
        keywords.add("ignore");
        keywords.add("immediate");
        keywords.add("in");
        keywords.add("index");
        keywords.add("indexed");
        keywords.add("initially");
        keywords.add("inner");
        keywords.add("insert");
        keywords.add("instead");
        keywords.add("intersect");
        keywords.add("into");
        keywords.add("is");
        keywords.add("isnull");
        keywords.add("join");
        keywords.add("key");
        keywords.add("last");
        keywords.add("left");
        keywords.add("like");
        keywords.add("limit");
        keywords.add("match");
        keywords.add("materialized");
        keywords.add("natural");
        keywords.add("no");
        keywords.add("not");
        keywords.add("nothing");
        keywords.add("notnull");
        keywords.add("null");
        keywords.add("nulls");
        keywords.add("of");
        keywords.add("offset");
        keywords.add("on");
        keywords.add("or");
        keywords.add("order");
        keywords.add("others");
        keywords.add("outer");
        keywords.add("over");
        keywords.add("partition");
        keywords.add("plan");
        keywords.add("pragma");
        keywords.add("preceding");
        keywords.add("primary");
        keywords.add("query");
        keywords.add("raise");
        keywords.add("range");
        keywords.add("recursive");
        keywords.add("references");
        keywords.add("regexp");
        keywords.add("reindex");
        keywords.add("release");
        keywords.add("rename");
        keywords.add("replace");
        keywords.add("restrict");
        keywords.add("returning");
        keywords.add("right");
        keywords.add("rollback");
        keywords.add("row");
        keywords.add("rows");
        keywords.add("savepoint");
        keywords.add("select");
        keywords.add("set");
        keywords.add("table");
        keywords.add("temp");
        keywords.add("temporary");
        keywords.add("then");
        keywords.add("ties");
        keywords.add("to");
        keywords.add("transaction");
        keywords.add("trigger");
        keywords.add("unbounded");
        keywords.add("union");
        keywords.add("unique");
        keywords.add("update");
        keywords.add("using");
        keywords.add("vacuum");
        keywords.add("values");
        keywords.add("view");
        keywords.add("virtual");
        keywords.add("when");
        keywords.add("where");
        keywords.add("window");
        keywords.add("with");
        keywords.add("without");
        RESERVED_SQL_KEYWORDS = Collections.unmodifiableSet(keywords);
    }

    private SqliteValidation() {
        // utility class
    }

    /**
     * Validates a field name for use as a SQLite identifier.
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
     * Returns the set of reserved SQL keywords for SQLite.
     *
     * @return an unmodifiable set of lowercased reserved keywords
     */
    static Set<String> getReservedKeywords() {
        return RESERVED_SQL_KEYWORDS;
    }
}
