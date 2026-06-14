package com.spandigital.cel2sql.dialect;

import com.spandigital.cel2sql.error.ConversionException;

import java.util.regex.Pattern;

/**
 * Shared ReDoS-safety validation for the RE2-style regex dialects
 * (PostgreSQL, MySQL, DuckDB, BigQuery, Spark).
 *
 * <p>Every dialect that accepts regular expressions enforces the same structural
 * limits to prevent catastrophic-backtracking attacks (CWE-1333): a maximum
 * pattern length, capture-group count, and nesting depth, plus heuristics that
 * reject nested quantifiers and quantified alternation. Those checks are
 * dialect-agnostic — they operate on the RE2 source pattern before any
 * dialect-specific conversion — so they live here once instead of being copied
 * into each {@code XxxRegex} class.</p>
 *
 * <p>Dialect-specific concerns (unsupported-feature detection, the actual
 * RE2-to-native conversion) remain in the per-dialect classes.</p>
 */
public final class RegexSafety {

    /** Maximum allowed regex pattern length. */
    public static final int MAX_PATTERN_LENGTH = 500;

    /** Maximum allowed capture groups in a pattern. */
    public static final int MAX_GROUPS = 20;

    /** Maximum allowed nesting depth of parenthesized groups. */
    public static final int MAX_NESTING_DEPTH = 10;

    private static final Pattern NESTED_QUANTIFIERS = Pattern.compile("[*+][*+]");
    private static final Pattern QUANTIFIED_ALTERNATION = Pattern.compile("\\([^)]*\\|[^)]*\\)[*+]");

    private RegexSafety() {
    }

    /**
     * Enforces the maximum pattern-length limit.
     *
     * @param pattern the RE2 regex pattern
     * @throws ConversionException if the pattern exceeds {@link #MAX_PATTERN_LENGTH}
     */
    public static void checkLength(String pattern) throws ConversionException {
        if (pattern.length() > MAX_PATTERN_LENGTH) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("pattern length %d exceeds limit of %d characters",
                            pattern.length(), MAX_PATTERN_LENGTH));
        }
    }

    /**
     * Runs the shared structural ReDoS checks against a pattern, in order:
     * <ol>
     *   <li>simple back-to-back quantifiers ({@code a*+}, {@code a++})</li>
     *   <li>quantified groups that themselves contain inner quantifiers ({@code (a+)+})</li>
     *   <li>capture-group count limit</li>
     *   <li>quantified alternation ({@code (a|b)+})</li>
     *   <li>group nesting-depth limit</li>
     * </ol>
     *
     * @param pattern the RE2 regex pattern (after any case-insensitivity flag has been stripped)
     * @throws ConversionException if any limit is exceeded or a catastrophic construct is detected
     */
    public static void checkReDoS(String pattern) throws ConversionException {
        if (NESTED_QUANTIFIERS.matcher(pattern).find()) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "regex contains catastrophic nested quantifiers that could cause ReDoS");
        }

        validateNoNestedQuantifiers(pattern);

        int groupCount = countUnescapedParens(pattern);
        if (groupCount > MAX_GROUPS) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("regex contains %d capture groups, exceeds limit of %d",
                            groupCount, MAX_GROUPS));
        }

        if (QUANTIFIED_ALTERNATION.matcher(pattern).find()) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "regex contains quantified alternation that could cause ReDoS");
        }

        int maxDepth = computeMaxNestingDepth(pattern);
        if (maxDepth > MAX_NESTING_DEPTH) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("nesting depth %d exceeds limit of %d", maxDepth, MAX_NESTING_DEPTH));
        }
    }

    /**
     * Validates that no quantified groups contain inner quantifiers (nested quantifiers).
     * This detects patterns like {@code (a+)+} that can cause catastrophic backtracking.
     */
    private static void validateNoNestedQuantifiers(String pattern) throws ConversionException {
        int depth = 0;
        boolean[] groupHasQuantifier = new boolean[pattern.length() + 1]; // oversized but safe
        int stackTop = -1;

        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);

            // Skip escaped characters
            if (i > 0 && pattern.charAt(i - 1) == '\\') {
                continue;
            }

            switch (ch) {
                case '(' -> {
                    depth++;
                    stackTop++;
                    groupHasQuantifier[stackTop] = false;
                }
                case ')' -> {
                    if (depth > 0) {
                        depth--;
                        if (i + 1 < pattern.length()) {
                            char next = pattern.charAt(i + 1);
                            if (next == '*' || next == '+' || next == '?' || next == '{') {
                                if (stackTop >= 0 && groupHasQuantifier[stackTop]) {
                                    throw new ConversionException(
                                            "Invalid pattern in expression",
                                            "regex contains catastrophic nested quantifiers that could cause ReDoS");
                                }
                            }
                        }
                        if (stackTop > 0 && groupHasQuantifier[stackTop]) {
                            groupHasQuantifier[stackTop - 1] = true;
                        }
                        if (stackTop >= 0) {
                            stackTop--;
                        }
                    }
                }
                case '*', '+', '?', '{' -> {
                    if (stackTop >= 0) {
                        groupHasQuantifier[stackTop] = true;
                    }
                }
            }
        }
    }

    /**
     * Counts the number of unescaped opening parentheses in the pattern.
     */
    private static int countUnescapedParens(String pattern) {
        int count = 0;
        for (int i = 0; i < pattern.length(); i++) {
            if (pattern.charAt(i) == '(' && (i == 0 || pattern.charAt(i - 1) != '\\')) {
                count++;
            }
        }
        return count;
    }

    /**
     * Computes the maximum nesting depth of parenthesized groups in the pattern.
     */
    private static int computeMaxNestingDepth(String pattern) {
        int maxDepth = 0;
        int currentDepth = 0;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '(' && (i == 0 || pattern.charAt(i - 1) != '\\')) {
                currentDepth++;
                if (currentDepth > maxDepth) {
                    maxDepth = currentDepth;
                }
            } else if (ch == ')' && (i == 0 || pattern.charAt(i - 1) != '\\')) {
                currentDepth--;
            }
        }
        return maxDepth;
    }
}
