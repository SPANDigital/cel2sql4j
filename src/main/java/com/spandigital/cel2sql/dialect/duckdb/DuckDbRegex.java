package com.spandigital.cel2sql.dialect.duckdb;

import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.dialect.RegexSafety;
import com.spandigital.cel2sql.error.ConversionException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Converts RE2 regex patterns to DuckDB's native regex format.
 * DuckDB uses RE2 natively, so minimal conversion is needed.
 * Performs security validation to prevent ReDoS attacks (CWE-1333).
 *
 * <p>Ported from the Go {@code dialect/duckdb/regex.go} implementation.</p>
 */
final class DuckDbRegex {

    private DuckDbRegex() {
    }

    /**
     * Converts an RE2 regex pattern to DuckDB's native regex format.
     * Since DuckDB uses RE2 natively, this primarily performs security validation
     * and handles the {@code (?i)} flag extraction.
     *
     * <p>Security checks performed (in order):
     * <ol>
     *   <li>Pattern length limit</li>
     *   <li>Validate pattern compiles</li>
     *   <li>Reject lookahead, lookbehind, named groups</li>
     *   <li>Detect catastrophic nested quantifiers</li>
     *   <li>Check nested quantifiers in groups</li>
     *   <li>Count and limit capture groups</li>
     *   <li>Detect exponential alternation patterns</li>
     *   <li>Check nesting depth</li>
     *   <li>Handle {@code (?i)} flag</li>
     *   <li>Reject other inline flags</li>
     *   <li>Convert non-capturing groups to plain groups</li>
     * </ol>
     *
     * @param re2Pattern the RE2 regex pattern
     * @return a {@link RegexResult} with the DuckDB-compatible pattern and case sensitivity flag
     * @throws ConversionException if the pattern is invalid or contains unsupported features
     */
    static RegexResult convertRE2ToDuckDB(String re2Pattern) throws ConversionException {
        // 1. Check pattern length
        RegexSafety.checkLength(re2Pattern);

        // 2. Validate pattern compiles
        try {
            Pattern.compile(re2Pattern);
        } catch (PatternSyntaxException e) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    String.format("regex pattern does not compile: %s", e.getDescription()));
        }

        // 3. Reject unsupported features: lookahead, lookbehind, named groups
        if (re2Pattern.contains("(?=") || re2Pattern.contains("(?!")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "lookahead assertions (?=...), (?!...) are not supported in DuckDB regex");
        }
        if (re2Pattern.contains("(?<=") || re2Pattern.contains("(?<!")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "lookbehind assertions (?<=...), (?<!...) are not supported in DuckDB regex");
        }
        if (re2Pattern.contains("(?P<")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "named capture groups (?P<name>...) are not supported in DuckDB regex");
        }

        // 4-8. Shared ReDoS safety checks (nested quantifiers, group count,
        //      quantified alternation, nesting depth)
        RegexSafety.checkReDoS(re2Pattern);

        // 9. Handle (?i) flag
        boolean caseInsensitive = false;
        String pattern = re2Pattern;
        if (pattern.startsWith("(?i)")) {
            caseInsensitive = true;
            pattern = pattern.substring(4);
        }

        // 10. Reject other inline flags
        if (pattern.contains("(?m") || pattern.contains("(?s") || pattern.contains("(?-")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "inline flags other than (?i) are not supported in DuckDB regex");
        }

        // 11. Convert non-capturing groups (?:...) to plain groups (...)
        pattern = pattern.replace("(?:", "(");

        return new RegexResult(pattern, caseInsensitive);
    }
}
