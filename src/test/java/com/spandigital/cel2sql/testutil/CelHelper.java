package com.spandigital.cel2sql.testutil;

import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import dev.cel.parser.CelStandardMacro;

/**
 * Helper class for compiling CEL expressions in tests.
 * Provides a standard compiler pre-configured with common variable declarations
 * and a convenience method for compiling CEL expression strings into checked ASTs.
 */
public final class CelHelper {
    private CelHelper() {}

    /** Creates a standard compiler with common variable declarations for most tests. */
    public static CelCompiler standardCompiler() {
        return CelCompilerFactory.standardCelCompilerBuilder()
            .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
            .addVar("name", SimpleType.STRING)
            .addVar("age", SimpleType.INT)
            .addVar("adult", SimpleType.BOOL)
            .addVar("height", SimpleType.DOUBLE)
            .addVar("email", SimpleType.STRING)
            .addVar("tags", ListType.create(SimpleType.STRING))
            .addVar("scores", ListType.create(SimpleType.INT))
            .addVar("salary", SimpleType.DOUBLE)
            .addVar("active", SimpleType.BOOL)
            .addVar("null_var", SimpleType.DYN)
            .addVar("string_list", ListType.create(SimpleType.STRING))
            .addVar("int_list", ListType.create(SimpleType.INT))
            .addVar("created_at", SimpleType.TIMESTAMP)
            .addVar("page", MapType.create(SimpleType.STRING, SimpleType.DYN))
            .build();
    }

    /** Compiles a CEL expression string using the standard compiler and returns the checked AST. */
    public static CelAbstractSyntaxTree compile(String celExpr) {
        return compile(standardCompiler(), celExpr);
    }

    /** Compiles a CEL expression string using the given compiler and returns the checked AST. */
    public static CelAbstractSyntaxTree compile(CelCompiler compiler, String celExpr) {
        try {
            var result = compiler.compile(celExpr);
            return result.getAst();
        } catch (CelValidationException e) {
            throw new RuntimeException("Failed to compile CEL expression: " + celExpr, e);
        }
    }
}
