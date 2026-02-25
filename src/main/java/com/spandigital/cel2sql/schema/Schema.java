package com.spandigital.cel2sql.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a table schema with O(1) field lookup.
 * Contains a list of fields for ordered iteration and a map index for fast lookups.
 */
public final class Schema {

    private final List<FieldSchema> fields;
    private final Map<String, FieldSchema> fieldIndex;

    /**
     * Creates a new Schema with field indexing for O(1) lookups.
     */
    public Schema(List<FieldSchema> fields) {
        Map<String, FieldSchema> index = new LinkedHashMap<>(fields.size());
        for (FieldSchema field : fields) {
            index.put(field.name(), field);
        }
        this.fields = Collections.unmodifiableList(fields);
        this.fieldIndex = Collections.unmodifiableMap(index);
    }

    /** Returns the ordered list of field schemas. */
    public List<FieldSchema> fields() {
        return fields;
    }

    /** Performs an O(1) lookup for a field by name. */
    public Optional<FieldSchema> findField(String name) {
        return Optional.ofNullable(fieldIndex.get(name));
    }

    /** Returns the number of fields in the schema. */
    public int len() {
        return fields.size();
    }
}
