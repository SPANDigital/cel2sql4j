package com.spandigital.cel2sql.integration;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Describes a single integration test case: a CEL expression, the category it belongs to,
 * and either expected row IDs (for WHERE clauses) or expression-only mode.
 */
public record IntegrationTestCase(
    String name,
    String celExpression,
    Set<Integer> expectedRowIds,
    boolean expressionOnly,
    String category
) {
    /** Creates a WHERE-clause test case that checks specific row IDs are returned. */
    static IntegrationTestCase where(String name, String cel, String category, int... ids) {
        Set<Integer> idSet = Arrays.stream(ids).boxed().collect(Collectors.toSet());
        return new IntegrationTestCase(name, cel, idSet, false, category);
    }

    /** Creates an expression-only test case that just verifies the SQL executes without error. */
    static IntegrationTestCase expr(String name, String cel, String category) {
        return new IntegrationTestCase(name, cel, Set.of(), true, category);
    }
}
