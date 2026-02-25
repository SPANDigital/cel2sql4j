package com.spandigital.cel2sql.dialect;

import com.spandigital.cel2sql.error.ConversionException;

/**
 * Functional interface for writing SQL fragments.
 * Used as callbacks in Dialect methods for writing sub-expressions.
 * This is the Java equivalent of Go's {@code func() error} callback pattern.
 */
@FunctionalInterface
public interface SqlWriter {
    void write() throws ConversionException;
}
