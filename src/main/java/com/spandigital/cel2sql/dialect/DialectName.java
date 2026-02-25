package com.spandigital.cel2sql.dialect;

/**
 * Supported SQL dialect names.
 */
public enum DialectName {
    POSTGRESQL("postgresql"),
    MYSQL("mysql"),
    SQLITE("sqlite"),
    DUCKDB("duckdb"),
    BIGQUERY("bigquery");

    private final String value;

    DialectName(String value) {
        this.value = value;
    }

    /** Returns the string representation of the dialect name. */
    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
