package com.spandigital.cel2sql.dialect.bigquery;

import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.dialect.RegexSafety;
import com.spandigital.cel2sql.error.ConversionException;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Converts RE2 regex patterns to BigQuery regex format.
 * BigQuery uses RE2 natively, so minimal conversion is needed.
 * Performs security validation to prevent ReDoS attacks (CWE-1333).
 *
 * <p>Ported from the Go {@code dialect/bigquery/regex.go} implementation.</p>
 */
final class BigQueryRegex {

    private BigQueryRegex() {
    }

    /**
     * Converts an RE2 regex pattern to BigQuery regex format.
     *
     * <p>BigQuery uses RE2 natively, so the pattern requires minimal conversion.
     * Security checks are still performed to prevent problematic patterns.</p>
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
     * @param re2Pattern the RE2 regex pattern
     * @return a {@link RegexResult} with the BigQuery pattern and case sensitivity flag
     * @throws ConversionException if the pattern is invalid or contains unsupported features
     */
    static RegexResult convertRE2ToBigQuery(String re2Pattern) throws ConversionException {
        // 1. Check pattern length
        RegexSafety.checkLength(re2Pattern);

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
                    "lookahead assertions (?=...), (?!...) are not supported in BigQuery regex");
        }
        if (re2Pattern.contains("(?<=") || re2Pattern.contains("(?<!")) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "lookbehind assertions (?<=...), (?<!...) are not supported in BigQuery regex");
        }
        if (re2Pattern.contains("(?P<")) {
            throw ConversionException.of(
                    "Invalid regex pattern",
                    "named capture groups (?P<name>...) are not supported in BigQuery regex");
        }

        // 4-8. Shared ReDoS safety checks (nested quantifiers, group count,
        //      quantified alternation, nesting depth)
        RegexSafety.checkReDoS(re2Pattern);

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
                    "inline flags other than (?i) are not supported in BigQuery regex");
        }

        // 11. Convert non-capturing groups (?:...) to plain groups (...)
        pattern = pattern.replace("(?:", "(");

        // BigQuery uses RE2 natively - \d, \w, \s, \b are all supported without conversion

        return new RegexResult(pattern, caseInsensitive);
    }
}
