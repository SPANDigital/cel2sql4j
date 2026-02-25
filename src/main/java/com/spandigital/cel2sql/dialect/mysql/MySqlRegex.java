package com.spandigital.cel2sql.dialect.mysql;

import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.error.ConversionException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Converts RE2 regex patterns to MySQL/ICU regex format.
 * Performs security validation to prevent ReDoS attacks (CWE-1333).
 *
 * <p>Ported from the Go {@code dialect/mysql/regex.go} implementation.</p>
 */
final class MySqlRegex {

    /** Maximum allowed regex pattern length. */
    static final int MAX_PATTERN_LENGTH = 500;

    /** Maximum allowed capture groups in a pattern. */
    static final int MAX_GROUPS = 20;

    /** Maximum allowed nesting depth of parenthesized groups. */
    static final int MAX_NESTING_DEPTH = 10;

    private static final Pattern NESTED_QUANTIFIERS = Pattern.compile("[*+][*+]");
    private static final Pattern QUANTIFIED_ALTERNATION = Pattern.compile("\\([^)]*\\|[^)]*\\)[*+]");

    private MySqlRegex() {
    }

    /**
     * Converts an RE2 regex pattern to MySQL/ICU regex format.
     *
     * <p>Security checks performed (in order):
     * <ol>
     *   <li>Pattern length limit</li>
     *   <li>Validate pattern compiles as a Java regex</li>
     *   <li>Reject lookahead, lookbehind, and named groups</li>
     *   <li>Detect catastrophic nested quantifiers</li>
     *   <li>Check nested quantifiers in groups</li>
     *   <li>Count and limit capture groups</li>
     *   <li>Detect exponential alternation</li>
     *   <li>Check nesting depth</li>
     *   <li>Handle {@code (?i)} flag</li>
     *   <li>Reject other inline flags</li>
     *   <li>Convert non-capturing groups to plain groups</li>
     * </ol>
     *
     * <p>MySQL ICU supports {@code \d}, {@code \w}, {@code \s}, {@code \b} natively,
     * so no POSIX class conversion is needed (unlike PostgreSQL).</p>
     *
     * @param re2Pattern the RE2 regex pattern
     * @return a {@link RegexResult} with the MySQL pattern and case sensitivity flag
     * @throws ConversionException if the pattern is invalid or contains unsupported features
     */
    static RegexResult convertRE2ToMySQL(String re2Pattern) throws ConversionException {
        // 1. Check pattern length
        if (re2Pattern.length() > MAX_PATTERN_LENGTH) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    String.format("pattern length %d exceeds limit of %d characters",
                            re2Pattern.length(), MAX_PATTERN_LENGTH));
        }

        // 2. Validate pattern compiles
        try {
            Pattern.compile(re2Pattern);
        } catch (PatternSyntaxException e) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "regex pattern does not compile: " + e.getDescription());
        }

        // 3. Reject lookahead, lookbehind, and named groups
        if (re2Pattern.contains("(?=") || re2Pattern.contains("(?!")) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "lookahead assertions (?=...), (?!...) are not supported in MySQL regex");
        }
        if (re2Pattern.contains("(?<=") || re2Pattern.contains("(?<!")) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "lookbehind assertions (?<=...), (?<!...) are not supported in MySQL regex");
        }
        if (re2Pattern.contains("(?P<")) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "named capture groups (?P<name>...) are not supported in MySQL regex");
        }

        // 4. Detect catastrophic nested quantifiers
        if (NESTED_QUANTIFIERS.matcher(re2Pattern).find()) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "regex contains catastrophic nested quantifiers that could cause ReDoS");
        }

        // 5. Check nested quantifiers in groups
        validateNoNestedQuantifiers(re2Pattern);

        // 6. Count and limit capture groups
        int groupCount = countUnescapedParens(re2Pattern);
        if (groupCount > MAX_GROUPS) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    String.format("regex contains %d capture groups, exceeds limit of %d",
                            groupCount, MAX_GROUPS));
        }

        // 7. Detect exponential alternation patterns
        if (QUANTIFIED_ALTERNATION.matcher(re2Pattern).find()) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "regex contains quantified alternation that could cause ReDoS");
        }

        // 8. Check nesting depth
        int maxDepth = computeMaxNestingDepth(re2Pattern);
        if (maxDepth > MAX_NESTING_DEPTH) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    String.format("nesting depth %d exceeds limit of %d", maxDepth, MAX_NESTING_DEPTH));
        }

        // 9. Handle (?i) flag -> set caseInsensitive=true, strip prefix
        boolean caseInsensitive = false;
        String pattern = re2Pattern;
        if (pattern.startsWith("(?i)")) {
            caseInsensitive = true;
            pattern = pattern.substring(4);
        }

        // 10. Reject other inline flags
        if (pattern.contains("(?m") || pattern.contains("(?s") || pattern.contains("(?-")) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "inline flags other than (?i) are not supported in MySQL regex");
        }

        // 11. Convert non-capturing groups (?:...) to plain groups (...)
        pattern = pattern.replace("(?:", "(");

        // MySQL ICU supports \d, \w, \s, \b natively - no conversion needed

        // 13. Return result
        return new RegexResult(pattern, caseInsensitive);
    }

    /**
     * Validates that no quantified groups contain inner quantifiers (nested quantifiers).
     * This detects patterns like {@code (a+)+} that can cause catastrophic backtracking.
     */
    private static void validateNoNestedQuantifiers(String pattern) throws ConversionException {
        int depth = 0;
        boolean[] groupHasQuantifier = new boolean[pattern.length()]; // oversized but safe
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
                                    throw ConversionException.of(
                                            "Invalid regex pattern",
                                            "regex contains catastrophic nested quantifiers that could cause ReDoS");
                                }
                            }
                        }
                        if (stackTop > 0) {
                            if (groupHasQuantifier[stackTop]) {
                                groupHasQuantifier[stackTop - 1] = true;
                            }
                        }
                        if (stackTop >= 0) {
                            stackTop--;
                        }
                    }
                }
                case '*', '+', '?' -> {
                    if (stackTop >= 0) {
                        groupHasQuantifier[stackTop] = true;
                    }
                }
                case '{' -> {
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
