package com.spandigital.cel2sql.error;

/**
 * Represents an error that occurred during CEL to SQL conversion.
 * Provides a sanitized user-facing message while preserving detailed information
 * for logging and debugging. This prevents information disclosure through error messages
 * (CWE-209: Information Exposure Through Error Message).
 */
public class ConversionException extends Exception {
    private final String userMessage;
    private final String internalDetails;

    public ConversionException(String userMessage, String internalDetails) {
        super(userMessage);
        this.userMessage = userMessage;
        this.internalDetails = internalDetails;
    }

    public ConversionException(String userMessage, String internalDetails, Throwable cause) {
        super(userMessage, cause);
        this.userMessage = userMessage;
        this.internalDetails = internalDetails;
    }

    /** Returns the sanitized user-facing error message. */
    public String getUserMessage() {
        return userMessage;
    }

    /**
     * Returns the full internal details for logging purposes.
     * This should only be used with structured logging, never displayed to users.
     */
    public String getInternalDetails() {
        return internalDetails != null && !internalDetails.isEmpty() ? internalDetails : userMessage;
    }

    @Override
    public String getMessage() {
        return userMessage;
    }

    /**
     * Creates a ConversionException with separate user and internal messages.
     */
    public static ConversionException of(String userMessage, String internalDetails) {
        return new ConversionException(userMessage, internalDetails);
    }

    /**
     * Creates a ConversionException with separate user and internal messages, wrapping a cause.
     */
    public static ConversionException of(String userMessage, String internalDetails, Throwable cause) {
        return new ConversionException(userMessage, internalDetails, cause);
    }

    /**
     * Wraps an existing exception with additional internal context.
     * Preserves specific user messages through wrapping chains: if the cause is
     * a ConversionException with a non-generic user message, that message is preserved.
     */
    public static ConversionException wrap(Throwable cause, String internalContext) {
        if (cause instanceof ConversionException ce) {
            String details = internalContext.isEmpty() ? ce.getInternalDetails()
                : internalContext + ": " + ce.getInternalDetails();
            if (!ce.getUserMessage().equals(ErrorMessages.CONVERSION_FAILED)) {
                return new ConversionException(ce.getUserMessage(), details, cause);
            }
            return new ConversionException(ErrorMessages.CONVERSION_FAILED, details, cause);
        }
        String details = internalContext.isEmpty() ? cause.getMessage()
            : internalContext + ": " + cause.getMessage();
        return new ConversionException(cause.getMessage(), details, cause);
    }
}
