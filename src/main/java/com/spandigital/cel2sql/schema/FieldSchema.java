package com.spandigital.cel2sql.schema;

import java.util.Collections;
import java.util.List;

/**
 * Represents a database field type with name, type, and optional nested schema.
 * This type is dialect-agnostic and used by all SQL dialect implementations.
 *
 * @param name        the field name
 * @param type        the SQL type name (text, integer, boolean, etc.)
 * @param repeated    true for array fields
 * @param dimensions  number of array dimensions (1 for integer[], 2 for integer[][], etc.)
 * @param schema      nested field schemas for composite types
 * @param isJSON      true for json/jsonb types
 * @param isJSONB     true for jsonb (vs json)
 * @param elementType for arrays: element type name
 */
public record FieldSchema(
    String name,
    String type,
    boolean repeated,
    int dimensions,
    List<FieldSchema> schema,
    boolean isJSON,
    boolean isJSONB,
    String elementType
) {
    /**
     * Canonical constructor that ensures the schema list is unmodifiable.
     */
    public FieldSchema {
        schema = schema != null
            ? Collections.unmodifiableList(schema)
            : Collections.emptyList();
        elementType = elementType != null ? elementType : "";
    }
}
