package com.spandigital.cel2sql.error;

/**
 * Centralized error message constants for CEL to SQL conversion.
 * These provide sanitized, user-safe messages that do not leak internal details.
 */
public final class ErrorMessages {
    private ErrorMessages() {}

    public static final String UNSUPPORTED_EXPRESSION = "Unsupported expression type";
    public static final String INVALID_OPERATOR = "Invalid operator in expression";
    public static final String UNSUPPORTED_TYPE = "Unsupported type in expression";
    public static final String UNSUPPORTED_COMPREHENSION = "Unsupported comprehension operation";
    public static final String COMPREHENSION_DEPTH_EXCEEDED = "Comprehension nesting exceeds maximum depth";
    public static final String INVALID_FIELD_ACCESS = "Invalid field access in expression";
    public static final String CONVERSION_FAILED = "Failed to convert expression component";
    public static final String INVALID_TIMESTAMP_OP = "Invalid timestamp operation";
    public static final String INVALID_DURATION = "Invalid duration value";
    public static final String INVALID_ARGUMENTS = "Invalid function arguments";
    public static final String INVALID_PATTERN = "Invalid pattern in expression";
}
