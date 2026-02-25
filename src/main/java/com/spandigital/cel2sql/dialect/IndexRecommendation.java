package com.spandigital.cel2sql.dialect;

/**
 * Represents a database index recommendation.
 * Provides actionable guidance for optimizing query performance.
 *
 * @param column     the database column that should be indexed
 * @param indexType  the index type (e.g., "BTREE", "GIN", "ART", "CLUSTERING")
 * @param expression the complete DDL statement that can be executed directly
 * @param reason     explains why this index is recommended
 */
public record IndexRecommendation(String column, String indexType, String expression, String reason) {
}
