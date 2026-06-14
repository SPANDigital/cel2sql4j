package com.spandigital.cel2sql.dialect.spark;

import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.dialect.RegexSafety;
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

    private SparkRegex() {}

    /**
     * Validates an RE2 regex pattern and returns it as-is for Spark.
     * Spark's java.util.regex engine handles inline {@code (?i)} natively, so
     * the returned {@link RegexResult#caseInsensitive()} is always {@code false}
     * — the engine will honour the inline flag if present.
     */
    static RegexResult convertRE2ToSpark(String re2Pattern) throws ConversionException {
        RegexSafety.checkLength(re2Pattern);
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
        // Shared ReDoS safety checks (nested quantifiers, group count,
        // quantified alternation, nesting depth)
        RegexSafety.checkReDoS(re2Pattern);

        if (re2Pattern.contains("(?m") || re2Pattern.contains("(?s") || re2Pattern.contains("(?-")) {
            throw new ConversionException(
                    "Invalid pattern in expression",
                    "inline flags other than (?i) are not supported in Spark regex");
        }
        return new RegexResult(re2Pattern, false);
    }
}
