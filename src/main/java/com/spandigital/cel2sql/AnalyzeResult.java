package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.IndexRecommendation;

import java.util.List;

/**
 * Result of query analysis containing both the SQL output and index recommendations.
 *
 * @param sql              the converted SQL WHERE clause
 * @param recommendations  list of index recommendations for query optimization
 */
public record AnalyzeResult(String sql, List<IndexRecommendation> recommendations) {
}
