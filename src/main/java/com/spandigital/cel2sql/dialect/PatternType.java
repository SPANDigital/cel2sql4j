package com.spandigital.cel2sql.dialect;

/**
 * Enumerates detected index-worthy query patterns.
 * Used by the index advisor to generate dialect-specific index recommendations.
 */
public enum PatternType {
    /** Equality/range comparisons (==, >, <, >=, <=) */
    COMPARISON,
    /** JSON/JSONB field access */
    JSON_ACCESS,
    /** Regex pattern matching */
    REGEX_MATCH,
    /** Array IN/containment */
    ARRAY_MEMBERSHIP,
    /** Array comprehension (all, exists, filter, map) */
    ARRAY_COMPREHENSION,
    /** JSON array comprehension */
    JSON_ARRAY_COMPREHENSION
}
