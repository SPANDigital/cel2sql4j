package com.spandigital.cel2sql;

import java.util.List;

/**
 * Represents the output of a CEL to SQL conversion with parameterized queries.
 * Contains the SQL string with placeholders ($1, $2, etc.) and the corresponding parameter values.
 *
 * @param sql        the generated SQL WHERE clause with placeholders
 * @param parameters parameter values in order ($1, $2, etc.)
 */
public record ConvertResult(String sql, List<Object> parameters) {
}
