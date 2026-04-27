package com.spandigital.cel2sql;

import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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
    private Set<String> jsonVariables;
    private Map<String, String> columnAliases;
    private int paramStartIndex;

    private ConvertOptions() {
        this.logger = DEFAULT_LOGGER;
        this.maxDepth = DEFAULT_MAX_DEPTH;
        this.maxOutputLength = DEFAULT_MAX_OUTPUT_LENGTH;
        this.jsonVariables = Collections.emptySet();
        this.columnAliases = Collections.emptyMap();
        this.paramStartIndex = 1;
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

    /**
     * Declares CEL variable names that correspond to flat JSONB columns.
     * When a variable is marked, both dot notation ({@code context.host}) and
     * bracket notation ({@code context["host"]}) emit JSONB text-extraction
     * operators (e.g. {@code context->>'host'}) instead of plain dot notation.
     */
    public ConvertOptions withJsonVariables(String... vars) {
        if (vars == null || vars.length == 0) {
            this.jsonVariables = Collections.emptySet();
            return this;
        }
        Set<String> set = new HashSet<>(vars.length);
        Collections.addAll(set, vars);
        this.jsonVariables = Collections.unmodifiableSet(set);
        return this;
    }

    /**
     * Maps CEL identifier names to SQL column names. When a CEL identifier
     * matches a key in this map, the SQL output uses the mapped column name.
     * Useful when database column names differ from user-facing CEL variables
     * (e.g. prefixed names like {@code usr_name}).
     */
    public ConvertOptions withColumnAliases(Map<String, String> aliases) {
        if (aliases == null || aliases.isEmpty()) {
            this.columnAliases = Collections.emptyMap();
            return this;
        }
        this.columnAliases = Collections.unmodifiableMap(new LinkedHashMap<>(aliases));
        return this;
    }

    /**
     * Sets the first placeholder index for {@link Cel2Sql#convertParameterized}.
     * Use this when embedding a CEL fragment into a larger parameterized query
     * so the placeholder numbers don't clash with existing parameters. Default
     * is 1. Values less than 1 are clamped to 1.
     */
    public ConvertOptions withParamStartIndex(int index) {
        this.paramStartIndex = Math.max(1, index);
        return this;
    }

    // --- Accessors ---

    public Map<String, Schema> schemas() { return schemas; }
    public Logger logger() { return logger; }
    public int maxDepth() { return maxDepth; }
    public int maxOutputLength() { return maxOutputLength; }
    public Dialect dialect() { return dialect; }
    public Set<String> jsonVariables() { return jsonVariables; }
    public Map<String, String> columnAliases() { return columnAliases; }
    public int paramStartIndex() { return paramStartIndex; }
}
