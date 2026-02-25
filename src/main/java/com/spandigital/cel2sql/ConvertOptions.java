package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Configuration options for CEL to SQL conversion.
 * Uses a builder pattern with {@link Consumer} for flexible configuration.
 */
public final class ConvertOptions {

    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(ConvertOptions.class);
    private static final int DEFAULT_MAX_DEPTH = 100;
    private static final int DEFAULT_MAX_OUTPUT_LENGTH = 50000;

    private Map<String, Schema> schemas;
    private Logger logger;
    private int maxDepth;
    private int maxOutputLength;
    private Dialect dialect;

    private ConvertOptions() {
        this.logger = DEFAULT_LOGGER;
        this.maxDepth = DEFAULT_MAX_DEPTH;
        this.maxOutputLength = DEFAULT_MAX_OUTPUT_LENGTH;
    }

    /**
     * Creates a new ConvertOptions with default settings, then applies the given configurator.
     */
    public static ConvertOptions configure(Consumer<ConvertOptions> configurator) {
        ConvertOptions options = new ConvertOptions();
        configurator.accept(options);
        return options;
    }

    /**
     * Creates a new ConvertOptions with default settings.
     */
    public static ConvertOptions defaults() {
        return new ConvertOptions();
    }

    /** Sets the schema map for JSON/JSONB field detection. */
    public ConvertOptions withSchemas(Map<String, Schema> schemas) {
        this.schemas = schemas;
        return this;
    }

    /** Sets the logger for observability and debugging. */
    public ConvertOptions withLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    /** Sets the maximum recursion depth. */
    public ConvertOptions withMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    /** Sets the maximum SQL output length. */
    public ConvertOptions withMaxOutputLength(int maxOutputLength) {
        this.maxOutputLength = maxOutputLength;
        return this;
    }

    /** Sets the SQL dialect. */
    public ConvertOptions withDialect(Dialect dialect) {
        this.dialect = dialect;
        return this;
    }

    // --- Accessors ---

    public Map<String, Schema> schemas() { return schemas; }
    public Logger logger() { return logger; }
    public int maxDepth() { return maxDepth; }
    public int maxOutputLength() { return maxOutputLength; }
    public Dialect dialect() { return dialect; }
}
