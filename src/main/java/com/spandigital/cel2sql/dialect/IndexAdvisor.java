package com.spandigital.cel2sql.dialect;

import java.util.List;

/**
 * Generates dialect-specific index recommendations.
 * Dialects that support index analysis implement this interface.
 */
public interface IndexAdvisor {

    /**
     * Generates an IndexRecommendation for the given pattern,
     * or returns null if the dialect has no applicable index for this pattern.
     */
    IndexRecommendation recommendIndex(IndexPattern pattern);

    /**
     * Returns which PatternTypes this advisor can handle.
     */
    List<PatternType> supportedPatterns();
}
