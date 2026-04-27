package com.spandigital.cel2sql.dialect.spark;

import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.error.ConversionException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Validates and converts RE2 regex patterns for Apache Spark SQL.
 * Spark uses java.util.regex (Java's Pattern engine), which is largely a
 * superset of RE2 for the safe subset cel2sql accepts. After security
 * validation, the pattern passes through unchanged — Spark's regex engine
 * honours inline {@code (?i)} natively, so this method always reports
 * {@code caseInsensitive=false} and lets the engine handle the flag.
 *
 * <p>Ported from the Go {@code dialect/spark/regex.go} implementation.
 * The validation logic is shared with the other RE2-style dialects (DuckDB,
 * PostgreSQL) and prevents ReDoS attacks (CWE-1333).</p>
 */
final class SparkRegex {

    /** Maximum allowed regex pattern length. */
    static final int MAX_PATTERN_LENGTH = 500;

    /** Maximum allowed capture groups in a pattern. */
    static final int MAX_GROUPS = 20;

    /** Maximum allowed nesting depth of parenthesized groups. */
    static final int MAX_NESTING_DEPTH = 10;

    private static final Pattern NESTED_QUANTIFIERS = Pattern.compile("[*+][*+]");
    private static final Pattern QUANTIFIED_ALTERNATION = Pattern.compile("\\([^)]*\\|[^)]*\\)[*+]");

    private SparkRegex() {}

    /**
     * Validates an RE2 regex pattern and returns it as-is for Spark.
     * Spark's java.util.regex engine handles inline {@code (?i)} natively, so
     * the returned {@link RegexResult#caseInsensitive()} is always {@code false}
     * — the engine will honour the inline flag if present.
     */
    static RegexResult convertRE2ToSpark(String re2Pattern) throws ConversionException {
        if (re2Pattern.length() > MAX_PATTERN_LENGTH) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("pattern length %d exceeds limit of %d characters",
                            re2Pattern.length(), MAX_PATTERN_LENGTH));
        }
        try {
            Pattern.compile(re2Pattern);
        } catch (PatternSyntaxException e) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "regex pattern does not compile: " + e.getDescription());
        }
        if (re2Pattern.contains("(?=") || re2Pattern.contains("(?!")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "lookahead assertions (?=...), (?!...) are not supported in Spark regex");
        }
        if (re2Pattern.contains("(?<=") || re2Pattern.contains("(?<!")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "lookbehind assertions (?<=...), (?<!...) are not supported in Spark regex");
        }
        if (re2Pattern.contains("(?P<")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "named capture groups (?P<name>...) are not supported in Spark regex");
        }
        if (NESTED_QUANTIFIERS.matcher(re2Pattern).find()) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "regex contains catastrophic nested quantifiers that could cause ReDoS");
        }
        validateNoNestedQuantifiers(re2Pattern);

        int groupCount = countUnescapedParens(re2Pattern);
        if (groupCount > MAX_GROUPS) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("regex contains %d capture groups, exceeds limit of %d",
                            groupCount, MAX_GROUPS));
        }
        if (QUANTIFIED_ALTERNATION.matcher(re2Pattern).find()) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "regex contains quantified alternation that could cause ReDoS");
        }
        int maxDepth = computeMaxNestingDepth(re2Pattern);
        if (maxDepth > MAX_NESTING_DEPTH) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("nesting depth %d exceeds limit of %d", maxDepth, MAX_NESTING_DEPTH));
        }
        if (re2Pattern.contains("(?m") || re2Pattern.contains("(?s") || re2Pattern.contains("(?-")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "inline flags other than (?i) are not supported in Spark regex");
        }
        return new RegexResult(re2Pattern, false);
    }

    private static void validateNoNestedQuantifiers(String pattern) throws ConversionException {
        int depth = 0;
        boolean[] groupHasQuantifier = new boolean[pattern.length() + 1];
        int stackTop = -1;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (i > 0 && pattern.charAt(i - 1) == '\\') continue;
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
                        if (stackTop >= 0) stackTop--;
                    }
                }
                case '*', '+', '?', '{' -> {
                    if (stackTop >= 0) groupHasQuantifier[stackTop] = true;
                }
            }
        }
    }

    private static int countUnescapedParens(String pattern) {
        int count = 0;
        for (int i = 0; i < pattern.length(); i++) {
            if (pattern.charAt(i) == '(' && (i == 0 || pattern.charAt(i - 1) != '\\')) {
                count++;
            }
        }
        return count;
    }

    private static int computeMaxNestingDepth(String pattern) {
        int maxDepth = 0;
        int currentDepth = 0;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '(' && (i == 0 || pattern.charAt(i - 1) != '\\')) {
                currentDepth++;
                if (currentDepth > maxDepth) maxDepth = currentDepth;
            } else if (ch == ')' && (i == 0 || pattern.charAt(i - 1) != '\\')) {
                currentDepth--;
            }
        }
        return maxDepth;
    }
}
