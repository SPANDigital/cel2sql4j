package com.spandigital.cel2sql.dialect;

/**
 * Describes a detected query pattern that could benefit from indexing.
 *
 * @param column    the full column name (e.g., "person.metadata")
 * @param pattern   the type of query pattern detected
 * @param tableHint optional table name hint for generating CREATE INDEX statements;
 *                  if null or empty, "table_name" is used as the default placeholder
 */
public record IndexPattern(String column, PatternType pattern, String tableHint) {

    public IndexPattern(String column, PatternType pattern) {
        this(column, pattern, null);
    }
}
