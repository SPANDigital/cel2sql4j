package com.spandigital.cel2sql.dialect;

/**
 * Result of converting an RE2 regex pattern to a dialect-specific format.
 *
 * @param pattern          the converted pattern in the dialect's native format
 * @param caseInsensitive  whether the match should be case-insensitive
 */
public record RegexResult(String pattern, boolean caseInsensitive) {
}
