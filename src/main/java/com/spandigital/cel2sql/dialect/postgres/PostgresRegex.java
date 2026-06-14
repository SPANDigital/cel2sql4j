package com.spandigital.cel2sql.dialect.postgres;

import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.dialect.RegexSafety;
import com.spandigital.cel2sql.error.ConversionException;

/**
 * Converts RE2 regex patterns to POSIX ERE format for PostgreSQL.
 * Performs security validation to prevent ReDoS attacks (CWE-1333).
 *
 * <p>Ported from the Go {@code dialect/postgres/regex.go} implementation.</p>
 */
final class PostgresRegex {

    private PostgresRegex() {
    }

    /**
     * Converts an RE2 regex pattern to POSIX ERE format for PostgreSQL.
     *
     * <p>Security checks performed (in order):
     * <ol>
     *   <li>Pattern length limit</li>
     *   <li>Extract {@code (?i)} flag</li>
     *   <li>Detect unsupported features: lookahead, lookbehind, named groups, inline flags</li>
     *   <li>Detect catastrophic nested quantifiers</li>
     *   <li>Count and limit capture groups</li>
     *   <li>Detect exponential alternation</li>
     *   <li>Check nesting depth</li>
     * </ol>
     *
     * <p>Conversions performed:
     * <ul>
     *   <li>{@code \b} to {@code \y}</li>
     *   <li>{@code \B} to {@code [^[:alnum:]_]}</li>
     *   <li>{@code \d} to {@code [[:digit:]]}</li>
     *   <li>{@code \D} to {@code [^[:digit:]]}</li>
     *   <li>{@code \w} to {@code [[:alnum:]_]}</li>
     *   <li>{@code \W} to {@code [^[:alnum:]_]}</li>
     *   <li>{@code \s} to {@code [[:space:]]}</li>
     *   <li>{@code \S} to {@code [^[:space:]]}</li>
     *   <li>{@code (?:} to {@code (}</li>
     * </ul>
     *
     * @param re2Pattern the RE2 regex pattern
     * @return a {@link RegexResult} with the POSIX pattern and case sensitivity flag
     * @throws ConversionException if the pattern is invalid or contains unsupported features
     */
    static RegexResult convertRE2ToPOSIX(String re2Pattern) throws ConversionException {
        // 1. Check pattern length
        RegexSafety.checkLength(re2Pattern);

        // 2. Extract case-insensitive flag
        boolean caseInsensitive = false;
        String pattern = re2Pattern;
        if (pattern.startsWith("(?i)")) {
            caseInsensitive = true;
            pattern = pattern.substring(4);
        }

        // 3. Detect unsupported features
        if (pattern.contains("(?=") || pattern.contains("(?!")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "lookahead assertions (?=...), (?!...) are not supported in PostgreSQL POSIX regex");
        }
        if (pattern.contains("(?<=") || pattern.contains("(?<!")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "lookbehind assertions (?<=...), (?<!...) are not supported in PostgreSQL POSIX regex");
        }
        if (pattern.contains("(?P<")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "named capture groups (?P<name>...) are not supported in PostgreSQL POSIX regex");
        }
        if (pattern.contains("(?m") || pattern.contains("(?s") || pattern.contains("(?-")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "inline flags other than (?i) are not supported in PostgreSQL POSIX regex");
        }

        // 4-7. Shared ReDoS safety checks (nested quantifiers, group count,
        //      quantified alternation, nesting depth)
        RegexSafety.checkReDoS(pattern);

        // 8. Convert RE2 to POSIX
        String posix = pattern;
        posix = posix.replace("\\b", "\\y");
        posix = posix.replace("\\B", "[^[:alnum:]_]");
        posix = posix.replace("\\d", "[[:digit:]]");
        posix = posix.replace("\\D", "[^[:digit:]]");
        posix = posix.replace("\\w", "[[:alnum:]_]");
        posix = posix.replace("\\W", "[^[:alnum:]_]");
        posix = posix.replace("\\s", "[[:space:]]");
        posix = posix.replace("\\S", "[^[:space:]]");
        posix = posix.replace("(?:", "(");

        return new RegexResult(posix, caseInsensitive);
    }
}
