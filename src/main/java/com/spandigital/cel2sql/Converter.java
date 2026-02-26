package com.spandigital.cel2sql;

import com.google.common.collect.ImmutableList;
import com.spandigital.cel2sql.dialect.Dialect;
import com.spandigital.cel2sql.dialect.IndexAdvisor;
import com.spandigital.cel2sql.dialect.IndexPattern;
import com.spandigital.cel2sql.dialect.IndexRecommendation;
import com.spandigital.cel2sql.dialect.PatternType;
import com.spandigital.cel2sql.dialect.RegexResult;
import com.spandigital.cel2sql.error.ConversionException;
import com.spandigital.cel2sql.error.ErrorMessages;
import com.spandigital.cel2sql.schema.FieldSchema;
import com.spandigital.cel2sql.schema.Schema;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.ast.CelConstant;
import dev.cel.common.ast.CelExpr;
import dev.cel.common.ast.CelExpr.CelCall;
import dev.cel.common.ast.CelExpr.CelComprehension;
import dev.cel.common.ast.CelExpr.CelList;
import dev.cel.common.ast.CelExpr.CelMap;
import dev.cel.common.ast.CelExpr.CelSelect;
import dev.cel.common.ast.CelExpr.CelStruct;
import dev.cel.common.ast.CelExpr.ExprKind.Kind;
import dev.cel.common.types.CelKind;
import dev.cel.common.types.CelType;
import dev.cel.common.types.ListType;
import dev.cel.common.types.MapType;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Core AST visitor that converts a checked CEL expression into a SQL WHERE clause.
 * This is the main workhorse of the cel2sql conversion process.
 *
 * <p>Ported from Go's {@code cel2sql.converter} struct in {@code cel2sql.go}.</p>
 *
 * <p>This class handles:
 * <ul>
 *   <li>All CEL expression kinds: calls, constants, identifiers, lists, selects, structs, maps, comprehensions</li>
 *   <li>Binary and unary operators with correct precedence and parenthesization</li>
 *   <li>String functions: contains, startsWith, endsWith, matches, size, charAt, indexOf, etc.</li>
 *   <li>Type casting: int(), string(), double(), bool(), etc.</li>
 *   <li>Timestamp/duration operations: getFullYear(), getMonth(), duration(), timestamp()</li>
 *   <li>JSON/JSONB field access and path building</li>
 *   <li>Array operations: indexing, size, in-operator</li>
 *   <li>Comprehensions: all, exists, exists_one, map, filter</li>
 *   <li>Parameterized query generation</li>
 * </ul>
 */
class Converter {

    // ========================================================================
    // CEL Operator Constants (from Go's cel-go operators package)
    // ========================================================================

    static final String CONDITIONAL = "_?_:_";
    static final String LOGICAL_AND = "_&&_";
    static final String LOGICAL_OR = "_||_";
    static final String LOGICAL_NOT = "!_";
    static final String NEGATE = "-_";
    static final String EQUALS = "_==_";
    static final String NOT_EQUALS = "_!=_";
    static final String LESS = "_<_";
    static final String LESS_EQUALS = "_<=_";
    static final String GREATER = "_>_";
    static final String GREATER_EQUALS = "_>=_";
    static final String ADD = "_+_";
    static final String SUBTRACT = "_-_";
    static final String MULTIPLY = "_*_";
    static final String DIVIDE = "_/_";
    static final String MODULO = "_%_";
    static final String INDEX = "_[_]";
    static final String IN = "@in";
    static final String OLD_IN = "_in_";
    static final String NOT_STRICTLY_FALSE = "@not_strictly_false";

    // ========================================================================
    // CEL Overload Constants (from Go's cel-go overloads package)
    // ========================================================================

    static final String CONTAINS = "contains";
    static final String STARTS_WITH = "startsWith";
    static final String ENDS_WITH = "endsWith";
    static final String MATCHES = "matches";
    static final String SIZE = "size";
    static final String TYPE_CONVERT_BOOL = "bool";
    static final String TYPE_CONVERT_BYTES = "bytes";
    static final String TYPE_CONVERT_DOUBLE = "double";
    static final String TYPE_CONVERT_INT = "int";
    static final String TYPE_CONVERT_STRING = "string";
    static final String TYPE_CONVERT_UINT = "uint";
    static final String TYPE_CONVERT_DURATION = "duration";
    static final String TYPE_CONVERT_TIMESTAMP = "timestamp";
    static final String TIME_GET_FULL_YEAR = "getFullYear";
    static final String TIME_GET_MONTH = "getMonth";
    static final String TIME_GET_DATE = "getDate";
    static final String TIME_GET_HOURS = "getHours";
    static final String TIME_GET_MINUTES = "getMinutes";
    static final String TIME_GET_SECONDS = "getSeconds";
    static final String TIME_GET_MILLISECONDS = "getMilliseconds";
    static final String TIME_GET_DAY_OF_YEAR = "getDayOfYear";
    static final String TIME_GET_DAY_OF_MONTH = "getDayOfMonth";
    static final String TIME_GET_DAY_OF_WEEK = "getDayOfWeek";

    // Additional string function overloads
    static final String LOWER_ASCII = "lowerAscii";
    static final String UPPER_ASCII = "upperAscii";
    static final String TRIM = "trim";
    static final String CHAR_AT = "charAt";
    static final String INDEX_OF = "indexOf";
    static final String LAST_INDEX_OF = "lastIndexOf";
    static final String SUBSTRING = "substring";
    static final String REPLACE = "replace";
    static final String REVERSE = "reverse";
    static final String SPLIT = "split";
    static final String JOIN = "join";

    // ========================================================================
    // Operator Precedence Map
    // ========================================================================

    private static final Map<String, Integer> PRECEDENCE_MAP;
    static {
        Map<String, Integer> m = new HashMap<>();
        m.put(CONDITIONAL, 8);
        m.put(LOGICAL_OR, 7);
        m.put(LOGICAL_AND, 6);
        m.put(EQUALS, 5);
        m.put(NOT_EQUALS, 5);
        m.put(LESS, 5);
        m.put(LESS_EQUALS, 5);
        m.put(GREATER, 5);
        m.put(GREATER_EQUALS, 5);
        m.put(IN, 5);
        m.put(OLD_IN, 5);
        m.put(ADD, 4);
        m.put(SUBTRACT, 4);
        m.put(MULTIPLY, 3);
        m.put(DIVIDE, 3);
        m.put(MODULO, 3);
        m.put(NEGATE, 2);
        m.put(INDEX, 1);
        PRECEDENCE_MAP = Collections.unmodifiableMap(m);
    }

    // ========================================================================
    // Reverse Binary Operator Map (CEL operator -> SQL symbol)
    // ========================================================================

    private static final Map<String, String> REVERSE_BINARY_OP_MAP;
    static {
        Map<String, String> m = new HashMap<>();
        m.put(ADD, "+");
        m.put(SUBTRACT, "-");
        m.put(MULTIPLY, "*");
        m.put(DIVIDE, "/");
        m.put(MODULO, "%");
        m.put(EQUALS, "=");
        m.put(NOT_EQUALS, "!=");
        m.put(LESS, "<");
        m.put(LESS_EQUALS, "<=");
        m.put(GREATER, ">");
        m.put(GREATER_EQUALS, ">=");
        REVERSE_BINARY_OP_MAP = Collections.unmodifiableMap(m);
    }

    // ========================================================================
    // Duration unit pattern for parsing Go-style duration strings
    // ========================================================================

    private static final Pattern DURATION_UNIT_PATTERN = Pattern.compile("(\\d+)(h|m(?!s)|s|ms|us|ns)");

    // ========================================================================
    // Maximum comprehension nesting depth
    // ========================================================================

    private static final int MAX_COMPREHENSION_DEPTH = 5;

    // ========================================================================
    // Instance Fields
    // ========================================================================

    private final CelAbstractSyntaxTree ast;
    private final StringBuilder str = new StringBuilder();
    private final Map<String, Schema> schemas;
    private final Logger logger;
    private final Dialect dialect;
    private final int maxDepth;
    private final int maxOutputLength;
    private final boolean parameterize;
    private final List<Object> parameters = new ArrayList<>();
    private int depth = 0;
    private int comprehensionDepth = 0;
    private int paramCount = 0;

    // ========================================================================
    // Constructor
    // ========================================================================

    Converter(CelAbstractSyntaxTree ast, ConvertOptions options, boolean parameterize) {
        this.ast = ast;
        this.schemas = options.schemas();
        this.logger = options.logger();
        this.dialect = options.dialect();
        this.maxDepth = options.maxDepth();
        this.maxOutputLength = options.maxOutputLength();
        this.parameterize = parameterize;
    }

    // ========================================================================
    // Public API
    // ========================================================================

    /**
     * Converts the AST to a SQL string.
     */
    String convert() throws ConversionException {
        visit(ast.getExpr());
        return str.toString();
    }

    /**
     * Returns the collected parameter values (for parameterized mode).
     */
    List<Object> getParameters() {
        return List.copyOf(parameters);
    }

    /**
     * Walks the AST to collect index recommendations without generating SQL.
     * This is used by the analysis engine.
     */
    List<IndexRecommendation> collectIndexRecommendations(IndexAdvisor advisor) {
        Map<String, IndexRecommendation> recommendations = new HashMap<>();
        analyzeExpr(ast.getExpr(), advisor, recommendations);
        return List.copyOf(recommendations.values());
    }

    private void analyzeExpr(CelExpr expr, IndexAdvisor advisor, Map<String, IndexRecommendation> recommendations) {
        if (expr == null) return;
        Kind kind = expr.getKind();
        switch (kind) {
            case CALL -> {
                CelCall call = expr.call();
                String fn = call.function();
                // Analyze comparison operators for index recommendations
                if (fn.equals(EQUALS) || fn.equals(NOT_EQUALS) || fn.equals(LESS) || fn.equals(LESS_EQUALS)
                        || fn.equals(GREATER) || fn.equals(GREATER_EQUALS)) {
                    if (call.args().size() >= 2) {
                        String col = extractColumnName(call.args().get(0));
                        if (col != null) {
                            addRecommendation(recommendations, advisor, col, PatternType.COMPARISON);
                        }
                    }
                }
                // Analyze regex matches
                if (fn.equals(MATCHES)) {
                    CelExpr target = call.target().isPresent() ? call.target().get() : (call.args().size() >= 1 ? call.args().get(0) : null);
                    if (target != null) {
                        String col = extractColumnName(target);
                        if (col != null) {
                            addRecommendation(recommendations, advisor, col, PatternType.REGEX_MATCH);
                        }
                    }
                }
                // Analyze IN operators
                if (fn.equals(IN) || fn.equals(OLD_IN)) {
                    if (call.args().size() >= 2) {
                        String col = extractColumnName(call.args().get(1));
                        if (col != null) {
                            addRecommendation(recommendations, advisor, col, PatternType.ARRAY_MEMBERSHIP);
                        }
                    }
                }
                // Recurse into target and arguments
                call.target().ifPresent(t -> analyzeExpr(t, advisor, recommendations));
                for (CelExpr arg : call.args()) {
                    analyzeExpr(arg, advisor, recommendations);
                }
            }
            case COMPREHENSION -> {
                CelComprehension comp = expr.comprehension();
                String col = extractColumnName(comp.iterRange());
                if (col != null) {
                    addRecommendation(recommendations, advisor, col, PatternType.ARRAY_COMPREHENSION);
                }
                analyzeExpr(comp.iterRange(), advisor, recommendations);
                analyzeExpr(comp.accuInit(), advisor, recommendations);
                analyzeExpr(comp.loopCondition(), advisor, recommendations);
                analyzeExpr(comp.loopStep(), advisor, recommendations);
                analyzeExpr(comp.result(), advisor, recommendations);
            }
            case SELECT -> {
                CelSelect sel = expr.select();
                analyzeExpr(sel.operand(), advisor, recommendations);
            }
            case LIST -> {
                CelList list = expr.list();
                for (CelExpr elem : list.elements()) {
                    analyzeExpr(elem, advisor, recommendations);
                }
            }
            default -> { /* CONSTANT, IDENT, etc. - nothing to analyze */ }
        }
    }

    private String extractColumnName(CelExpr expr) {
        if (expr == null) return null;
        if (expr.getKind() == Kind.IDENT) {
            return expr.ident().name();
        }
        if (expr.getKind() == Kind.SELECT) {
            CelSelect sel = expr.select();
            String operandName = extractColumnName(sel.operand());
            if (operandName != null) {
                return operandName + "." + sel.field();
            }
            return sel.field();
        }
        return null;
    }

    private void addRecommendation(Map<String, IndexRecommendation> recommendations, IndexAdvisor advisor, String column, PatternType pattern) {
        IndexRecommendation rec = advisor.recommendIndex(new IndexPattern(column, pattern));
        if (rec == null) return;
        IndexRecommendation existing = recommendations.get(column);
        if (existing == null) {
            recommendations.put(column, rec);
        } else {
            // More specialized index types take priority over basic ones
            if (isBasicIndexType(existing.indexType()) && !isBasicIndexType(rec.indexType())) {
                recommendations.put(column, rec);
            }
        }
    }

    private static boolean isBasicIndexType(String indexType) {
        return "BTREE".equals(indexType) || "ART".equals(indexType) || "CLUSTERING".equals(indexType);
    }

    // ========================================================================
    // Core Visitor Dispatch
    // ========================================================================

    /**
     * Main visitor dispatch. Routes to the appropriate visitXxx method based on expression kind.
     */
    private void visit(CelExpr expr) throws ConversionException {
        depth++;
        try {
            if (depth > maxDepth) {
                throw new ConversionException(
                        ErrorMessages.CONVERSION_FAILED,
                        "maximum recursion depth " + maxDepth + " exceeded");
            }
            if (str.length() > maxOutputLength) {
                throw new ConversionException(
                        ErrorMessages.CONVERSION_FAILED,
                        "output length exceeds maximum of " + maxOutputLength + " characters");
            }
            Kind kind = expr.getKind();
            switch (kind) {
                case CALL -> visitCall(expr);
                case COMPREHENSION -> visitComprehension(expr);
                case CONSTANT -> visitConst(expr);
                case IDENT -> visitIdent(expr);
                case LIST -> visitList(expr);
                case SELECT -> visitSelect(expr);
                case STRUCT -> visitStruct(expr);
                case MAP -> visitStructMap(expr);
                default -> throw new ConversionException(
                        ErrorMessages.UNSUPPORTED_EXPRESSION,
                        "unsupported expression kind: " + kind);
            }
        } finally {
            depth--;
        }
    }

    // ========================================================================
    // Type Helpers
    // ========================================================================

    /**
     * Gets the type of an expression from the AST type map.
     */
    private CelType getType(CelExpr expr) {
        return ast.getType(expr.id()).orElse(null);
    }

    /**
     * Checks if a type represents a string.
     */
    private boolean isStringType(CelType type) {
        return type != null && type.kind() == CelKind.STRING;
    }

    /**
     * Checks if a type represents a list/array.
     */
    private boolean isListType(CelType type) {
        return type != null && type.kind() == CelKind.LIST;
    }

    /**
     * Checks if a type represents a map.
     */
    private boolean isMapType(CelType type) {
        return type != null && type.kind() == CelKind.MAP;
    }

    /**
     * Checks if a type represents a timestamp.
     */
    private boolean isTimestampType(CelType type) {
        return type != null && type.kind() == CelKind.TIMESTAMP;
    }

    /**
     * Checks if a type represents a duration.
     */
    private boolean isDurationType(CelType type) {
        return type != null && type.kind() == CelKind.DURATION;
    }

    /**
     * Checks if a type is numeric (int, uint, double).
     */
    private boolean isNumericType(CelType type) {
        if (type == null) return false;
        return type.kind() == CelKind.INT || type.kind() == CelKind.UINT || type.kind() == CelKind.DOUBLE;
    }

    /**
     * Checks if a comparison involves numeric types on both sides.
     */
    private boolean isNumericComparison(CelExpr lhs, CelExpr rhs) {
        CelType lhsType = getType(lhs);
        CelType rhsType = getType(rhs);
        return isNumericType(lhsType) && isNumericType(rhsType);
    }

    // ========================================================================
    // Literal Detection Helpers
    // ========================================================================

    /**
     * Checks if an expression is a null literal.
     */
    private boolean isNullLiteral(CelExpr expr) {
        return expr.getKind() == Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.NULL_VALUE;
    }

    /**
     * Checks if an expression is a boolean literal (true or false).
     */
    private boolean isBoolLiteral(CelExpr expr) {
        return expr.getKind() == Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.BOOLEAN_VALUE;
    }

    /**
     * Checks if an expression is a string literal.
     */
    private boolean isStringLiteral(CelExpr expr) {
        return expr.getKind() == Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.STRING_VALUE;
    }

    /**
     * Checks if an expression is an int64 literal with value 0.
     */
    private boolean isIntZero(CelExpr expr) {
        return expr.getKind() == Kind.CONSTANT
                && expr.constant().getKind() == CelConstant.Kind.INT64_VALUE
                && expr.constant().int64Value() == 0;
    }

    /**
     * Checks if an expression is a boolean literal true.
     */
    private boolean isBoolTrue(CelExpr expr) {
        return isBoolLiteral(expr) && expr.constant().booleanValue();
    }

    /**
     * Checks if an expression is a boolean literal false.
     */
    private boolean isBoolFalse(CelExpr expr) {
        return isBoolLiteral(expr) && !expr.constant().booleanValue();
    }

    /**
     * Checks if an expression is an empty list literal.
     */
    private boolean isEmptyList(CelExpr expr) {
        return expr.getKind() == Kind.LIST && expr.list().elements().isEmpty();
    }

    /**
     * Checks if an expression is a field access (identifier or select).
     */
    private boolean isFieldAccessExpression(CelExpr expr) {
        return expr.getKind() == Kind.IDENT || expr.getKind() == Kind.SELECT;
    }

    // ========================================================================
    // visitConst - Constant Literals
    // ========================================================================

    /**
     * Visits a constant expression and writes the SQL literal.
     */
    private void visitConst(CelExpr expr) throws ConversionException {
        CelConstant c = expr.constant();
        switch (c.getKind()) {
            case BOOLEAN_VALUE -> str.append(c.booleanValue() ? "TRUE" : "FALSE");
            case NULL_VALUE -> str.append("NULL");
            case INT64_VALUE -> {
                if (parameterize) {
                    writeParam(c.int64Value());
                } else {
                    str.append(c.int64Value());
                }
            }
            case UINT64_VALUE -> {
                long val = c.uint64Value().longValue();
                if (parameterize) {
                    writeParam(val);
                } else {
                    str.append(val);
                }
            }
            case DOUBLE_VALUE -> {
                if (parameterize) {
                    writeParam(c.doubleValue());
                } else {
                    str.append(c.doubleValue());
                }
            }
            case STRING_VALUE -> {
                if (parameterize) {
                    writeParam(c.stringValue());
                } else {
                    dialect.writeStringLiteral(str, c.stringValue());
                }
            }
            case BYTES_VALUE -> {
                byte[] bytes = c.bytesValue().toByteArray();
                if (parameterize) {
                    writeParam(bytes);
                } else {
                    dialect.writeBytesLiteral(str, bytes);
                }
            }
            default -> throw new ConversionException(
                    ErrorMessages.UNSUPPORTED_TYPE,
                    "unsupported constant kind: " + c.getKind());
        }
    }

    /**
     * Writes a parameter placeholder and records the value.
     */
    private void writeParam(Object value) {
        paramCount++;
        parameters.add(value);
        dialect.writeParamPlaceholder(str, paramCount);
    }

    // ========================================================================
    // visitIdent - Identifiers
    // ========================================================================

    /**
     * Visits an identifier expression, validates it, and writes the SQL identifier.
     */
    private void visitIdent(CelExpr expr) throws ConversionException {
        String name = expr.ident().name();
        dialect.validateFieldName(name);
        str.append(name);
    }

    // ========================================================================
    // visitSelect - Field Selection (including JSON paths)
    // ========================================================================

    /**
     * Visits a select (field access) expression. Handles:
     * - Regular field access: table.field
     * - has() macro: field presence test
     * - JSON path access: json_col->'field'->>'nested'
     */
    private void visitSelect(CelExpr expr) throws ConversionException {
        CelSelect sel = expr.select();

        // Handle has() macro
        if (sel.testOnly()) {
            visitHasField(expr);
            return;
        }

        // Check for JSON path access
        if (shouldUseJSONPath(expr)) {
            visitJSONSelect(expr);
            return;
        }

        // Regular field access: operand.field
        CelExpr operand = sel.operand();
        String field = sel.field();
        dialect.validateFieldName(field);

        // If the operand is an ident, this is table.field or simple field access
        if (operand.getKind() == Kind.IDENT) {
            visit(operand);
            str.append('.');
            str.append(field);
        } else {
            visit(operand);
            str.append('.');
            str.append(field);
        }
    }

    /**
     * Handles the has() macro which tests for field presence.
     * For JSON fields, generates: json_col ? 'field' (JSONB) or json_col->'field' IS NOT NULL (JSON)
     * For regular fields, generates: field IS NOT NULL
     */
    private void visitHasField(CelExpr expr) throws ConversionException {
        CelSelect sel = expr.select();
        CelExpr operand = sel.operand();
        String field = sel.field();

        // Check if this is a JSON field
        TableAndField tf = getTableAndFieldFromSelectChain(operand);
        if (tf != null) {
            FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
            if (fieldSchema != null && fieldSchema.isJSON()) {
                boolean isJSONB = fieldSchema.isJSONB();
                // Check for nested JSON path
                if (hasJSONFieldInChain(operand) && operand.getKind() == Kind.SELECT) {
                    // Nested JSON: build a path and use jsonb_extract_path_text IS NOT NULL
                    List<String> path = new ArrayList<>();
                    path.add(field);
                    CelExpr root = buildJSONPathInternal(operand, path);
                    dialect.writeJSONExtractPath(str, path, () -> visit(root));
                    return;
                }
                dialect.writeJSONExistence(str, isJSONB, field, () -> visit(operand));
                return;
            }
        }

        // Regular has(): field IS NOT NULL
        visit(operand);
        str.append('.');
        str.append(field);
        str.append(" IS NOT NULL");
    }

    /**
     * Visits a JSON field access, building the appropriate -> or ->> operator chain.
     */
    private void visitJSONSelect(CelExpr expr) throws ConversionException {
        CelSelect sel = expr.select();
        boolean isFinal = isJSONTextExtraction(expr);

        if (isNestedJSONAccess(sel.operand())) {
            // Intermediate JSON access: use ->
            dialect.writeJSONFieldAccess(str,
                    () -> visitJSONSelect(sel.operand()),
                    sel.field(),
                    isFinal);
        } else {
            // First level JSON access
            dialect.writeJSONFieldAccess(str,
                    () -> visit(sel.operand()),
                    sel.field(),
                    isFinal);
        }
    }

    // ========================================================================
    // visitCall - Function Calls and Operators
    // ========================================================================

    /**
     * Dispatches call expressions based on the function name.
     */
    private void visitCall(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();

        switch (fun) {
            case CONDITIONAL -> visitCallConditional(expr);
            case LOGICAL_AND, LOGICAL_OR -> visitCallBinary(expr);
            case EQUALS, NOT_EQUALS -> visitCallEqualityOp(expr);
            case LESS, LESS_EQUALS, GREATER, GREATER_EQUALS -> visitCallComparisonOp(expr);
            case ADD -> visitCallAdd(expr);
            case SUBTRACT -> visitCallSubtract(expr);
            case MULTIPLY, DIVIDE, MODULO -> visitCallBinary(expr);
            case LOGICAL_NOT -> visitCallUnary(expr);
            case NEGATE -> visitCallNegate(expr);
            case INDEX -> visitCallIndex(expr);
            case IN, OLD_IN -> visitCallIn(expr);
            case NOT_STRICTLY_FALSE -> {
                // Unwrap @not_strictly_false - just visit the inner expression
                if (!call.args().isEmpty()) {
                    visit(call.args().get(0));
                }
            }
            default -> visitCallFunc(expr);
        }
    }

    // ========================================================================
    // visitCallBinary - Binary Operators
    // ========================================================================

    /**
     * Visits a binary operator call (AND, OR, arithmetic, comparison).
     * Handles parenthesization based on operator precedence.
     */
    private void visitCallBinary(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);

        String sqlOp;
        switch (fun) {
            case LOGICAL_AND -> sqlOp = "AND";
            case LOGICAL_OR -> sqlOp = "OR";
            default -> {
                sqlOp = REVERSE_BINARY_OP_MAP.get(fun);
                if (sqlOp == null) {
                    throw new ConversionException(
                            ErrorMessages.INVALID_OPERATOR,
                            "unknown binary operator: " + fun);
                }
            }
        }

        visitMaybeNested(expr, lhs);
        str.append(' ').append(sqlOp).append(' ');
        visitMaybeNested(expr, rhs);
    }

    // ========================================================================
    // Equality Operators (==, !=) with IS NULL / IS TRUE / IS FALSE handling
    // ========================================================================

    /**
     * Handles equality/inequality with special cases for NULL, TRUE, FALSE.
     */
    private void visitCallEqualityOp(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);
        boolean isEquals = EQUALS.equals(fun);

        // Handle NULL comparisons: IS NULL / IS NOT NULL
        if (isNullLiteral(rhs)) {
            visitMaybeNested(expr, lhs);
            str.append(isEquals ? " IS NULL" : " IS NOT NULL");
            return;
        }
        if (isNullLiteral(lhs)) {
            visitMaybeNested(expr, rhs);
            str.append(isEquals ? " IS NULL" : " IS NOT NULL");
            return;
        }

        // Handle boolean comparisons: IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE
        if (isBoolLiteral(rhs)) {
            boolean val = rhs.constant().booleanValue();
            visitMaybeNested(expr, lhs);
            if (isEquals) {
                str.append(val ? " IS TRUE" : " IS FALSE");
            } else {
                str.append(val ? " IS NOT TRUE" : " IS NOT FALSE");
            }
            return;
        }
        if (isBoolLiteral(lhs)) {
            boolean val = lhs.constant().booleanValue();
            visitMaybeNested(expr, rhs);
            if (isEquals) {
                str.append(val ? " IS TRUE" : " IS FALSE");
            } else {
                str.append(val ? " IS NOT TRUE" : " IS NOT FALSE");
            }
            return;
        }

        // Handle JSON field comparison with numeric cast
        if (isJSONFieldRequiringCast(lhs, rhs) || isJSONFieldRequiringCast(rhs, lhs)) {
            visitJSONComparisonWithCast(expr, lhs, rhs, isEquals ? "=" : "!=");
            return;
        }

        // Regular equality
        visitMaybeNested(expr, lhs);
        str.append(isEquals ? " = " : " != ");
        visitMaybeNested(expr, rhs);
    }

    /**
     * Handles comparison operators (<, <=, >, >=) with numeric cast for JSON fields.
     */
    private void visitCallComparisonOp(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);
        String sqlOp = REVERSE_BINARY_OP_MAP.get(fun);

        // Handle JSON field comparison with numeric cast
        if (isJSONFieldRequiringCast(lhs, rhs) || isJSONFieldRequiringCast(rhs, lhs)) {
            visitJSONComparisonWithCast(expr, lhs, rhs, sqlOp);
            return;
        }

        visitMaybeNested(expr, lhs);
        str.append(' ').append(sqlOp).append(' ');
        visitMaybeNested(expr, rhs);
    }

    /**
     * Checks if a JSON field access needs a numeric cast for comparison.
     */
    private boolean isJSONFieldRequiringCast(CelExpr field, CelExpr other) {
        if (!shouldUseJSONPath(field)) return false;
        CelType otherType = getType(other);
        return isNumericType(otherType);
    }

    /**
     * Writes a JSON comparison with a ::numeric cast on the JSON extraction side.
     */
    private void visitJSONComparisonWithCast(CelExpr parent, CelExpr lhs, CelExpr rhs, String op) throws ConversionException {
        boolean lhsIsJSON = shouldUseJSONPath(lhs);
        if (lhsIsJSON) {
            str.append('(');
            visitMaybeNested(parent, lhs);
            str.append(')');
            dialect.writeCastToNumeric(str);
        } else {
            visitMaybeNested(parent, lhs);
        }
        str.append(' ').append(op).append(' ');
        if (!lhsIsJSON && shouldUseJSONPath(rhs)) {
            str.append('(');
            visitMaybeNested(parent, rhs);
            str.append(')');
            dialect.writeCastToNumeric(str);
        } else {
            visitMaybeNested(parent, rhs);
        }
    }

    // ========================================================================
    // Add operator (handles string concat and timestamp arithmetic)
    // ========================================================================

    /**
     * Handles the + operator. Dispatches to string concat, timestamp arithmetic, or regular add.
     */
    private void visitCallAdd(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);

        CelType lhsType = getType(lhs);
        CelType rhsType = getType(rhs);

        // String concatenation
        if (isStringType(lhsType) && isStringType(rhsType)) {
            dialect.writeStringConcat(str,
                    () -> visitMaybeNested(expr, lhs),
                    () -> visitMaybeNested(expr, rhs));
            return;
        }

        // Timestamp + duration
        if (isTimestampType(lhsType) && isDurationType(rhsType)) {
            callTimestampOperation(lhs, rhs, "+");
            return;
        }

        // Duration + timestamp
        if (isDurationType(lhsType) && isTimestampType(rhsType)) {
            callTimestampOperation(rhs, lhs, "+");
            return;
        }

        // List concatenation: use || for arrays
        if (isListType(lhsType) && isListType(rhsType)) {
            visitMaybeNested(expr, lhs);
            str.append(" || ");
            visitMaybeNested(expr, rhs);
            return;
        }

        // Regular arithmetic addition
        visitCallBinary(expr);
    }

    /**
     * Handles the - operator. Dispatches to timestamp arithmetic or regular subtract.
     */
    private void visitCallSubtract(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);

        CelType lhsType = getType(lhs);
        CelType rhsType = getType(rhs);

        // Timestamp - duration
        if (isTimestampType(lhsType) && isDurationType(rhsType)) {
            callTimestampOperation(lhs, rhs, "-");
            return;
        }

        // Regular arithmetic subtraction
        visitCallBinary(expr);
    }

    // ========================================================================
    // Conditional (ternary) operator
    // ========================================================================

    /**
     * Converts CEL ternary {@code a ? b : c} to SQL {@code CASE WHEN a THEN b ELSE c END}.
     */
    private void visitCallConditional(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr condition = call.args().get(0);
        CelExpr trueExpr = call.args().get(1);
        CelExpr falseExpr = call.args().get(2);

        str.append("CASE WHEN ");
        visit(condition);
        str.append(" THEN ");
        visit(trueExpr);
        str.append(" ELSE ");
        visit(falseExpr);
        str.append(" END");
    }

    // ========================================================================
    // Unary Operators
    // ========================================================================

    /**
     * Converts CEL logical not {@code !x} to SQL {@code NOT x}.
     */
    private void visitCallUnary(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr arg = call.args().get(0);
        str.append("NOT ");
        visitMaybeNested(expr, arg);
    }

    /**
     * Converts CEL negation {@code -x} to SQL {@code -x}.
     */
    private void visitCallNegate(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr arg = call.args().get(0);
        str.append('-');
        visitMaybeNested(expr, arg);
    }

    // ========================================================================
    // Index Operator (array[i] and map["key"])
    // ========================================================================

    /**
     * Handles the index operator: arr[i] or map["key"].
     * Routes to list index or map index based on the operand type.
     */
    private void visitCallIndex(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr operand = call.args().get(0);
        CelExpr index = call.args().get(1);

        CelType operandType = getType(operand);

        if (isListType(operandType)) {
            visitCallListIndex(operand, index);
        } else if (isMapType(operandType)) {
            visitCallMapIndex(expr, operand, index);
        } else {
            // Check for JSON array access
            if (shouldUseJSONPath(operand)) {
                // JSON array: operand->index
                visit(operand);
                str.append("->");
                visit(index);
                return;
            }
            // Default: treat as list index
            visitCallListIndex(operand, index);
        }
    }

    /**
     * Writes a list (array) index expression. Uses dialect-specific 0-to-1 index conversion.
     */
    private void visitCallListIndex(CelExpr operand, CelExpr index) throws ConversionException {
        // Check for JSON array field
        TableAndField tf = getTableAndFieldFromSelectChain(operand);
        if (tf != null) {
            FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
            if (fieldSchema != null && (fieldSchema.isJSON() || fieldSchema.isJSONB())) {
                // JSON array index: operand->index
                visit(operand);
                str.append("->");
                visit(index);
                return;
            }
        }

        // Check if index is a constant int for optimized output
        if (index.getKind() == Kind.CONSTANT && index.constant().getKind() == CelConstant.Kind.INT64_VALUE) {
            long idx = index.constant().int64Value();
            dialect.writeListIndexConst(str, () -> visit(operand), idx);
        } else {
            dialect.writeListIndex(str, () -> visit(operand), () -> visit(index));
        }
    }

    /**
     * Writes a map index expression, converting to JSON field access if needed.
     */
    private void visitCallMapIndex(CelExpr expr, CelExpr operand, CelExpr index) throws ConversionException {
        // For map types, we treat map["key"] as a JSON field access if applicable
        if (isStringLiteral(index)) {
            String key = index.constant().stringValue();
            dialect.validateFieldName(key);
            // If operand is a JSON field, use JSON access syntax
            if (shouldUseJSONPath(operand)) {
                boolean isFinal = isJSONTextExtraction(expr);
                dialect.writeJSONFieldAccess(str, () -> visit(operand), key, isFinal);
                return;
            }
        }
        // Default: array-style index
        visit(operand);
        str.append('[');
        visit(index);
        str.append(']');
    }

    // ========================================================================
    // IN Operator
    // ========================================================================

    /**
     * Handles the 'in' operator: elem in list.
     * Supports: regular arrays, JSON arrays, and literal lists.
     */
    private void visitCallIn(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr elem = call.args().get(0);
        CelExpr collection = call.args().get(1);

        CelType collectionType = getType(collection);

        // Check if the collection is a JSON array field
        if (shouldUseJSONPath(collection)) {
            visitInJSONArray(elem, collection);
            return;
        }

        // Check if the collection is a regular array field
        if (isListType(collectionType)) {
            // Check for JSON array in schema
            TableAndField tf = getTableAndFieldFromSelectChain(collection);
            if (tf != null) {
                FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
                if (fieldSchema != null && (fieldSchema.isJSON() || fieldSchema.isJSONB())) {
                    visitInJSONArray(elem, collection);
                    return;
                }
            }

            // Regular array: elem = ANY(array)
            dialect.writeArrayMembership(str,
                    () -> visit(elem),
                    () -> visit(collection));
            return;
        }

        // Map: check key existence via ? operator or similar
        if (isMapType(collectionType)) {
            // For maps, "key in map" becomes: map ? 'key'
            if (shouldUseJSONPath(collection) || isJSONObjectFieldAccess(collection)) {
                dialect.writeJSONExistence(str, true, getStringValue(elem), () -> visit(collection));
                return;
            }
        }

        // Inline list: elem IN (val1, val2, ...)
        if (collection.getKind() == Kind.LIST) {
            CelList list = collection.list();
            ImmutableList<CelExpr> elements = list.elements();
            if (elements.isEmpty()) {
                str.append("FALSE");
                return;
            }
            visit(elem);
            str.append(" IN (");
            for (int i = 0; i < elements.size(); i++) {
                if (i > 0) str.append(", ");
                visit(elements.get(i));
            }
            str.append(')');
            return;
        }

        // Default: use = ANY()
        dialect.writeArrayMembership(str,
                () -> visit(elem),
                () -> visit(collection));
    }

    /**
     * Handles IN for JSON arrays: elem = ANY(ARRAY(SELECT jsonb_array_elements_text(...)))
     */
    private void visitInJSONArray(CelExpr elem, CelExpr collection) throws ConversionException {
        TableAndField tf = getTableAndFieldFromSelectChain(collection);
        if (tf != null) {
            FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
            if (fieldSchema != null) {
                boolean isJSONB = fieldSchema.isJSONB();
                String jsonFunc = getJSONArrayFunction(isJSONB, true);
                visit(elem);
                str.append(" = ");
                dialect.writeJSONArrayMembership(str, jsonFunc, () -> visit(collection));
                return;
            }
        }

        // If no schema found, try nested JSON access
        if (isNestedJSONAccess(collection)) {
            visit(elem);
            str.append(" = ");
            dialect.writeNestedJSONArrayMembership(str, () -> visit(collection));
            return;
        }

        // Fallback to standard array membership
        dialect.writeArrayMembership(str,
                () -> visit(elem),
                () -> visit(collection));
    }

    // ========================================================================
    // visitCallFunc - Named Function Calls
    // ========================================================================

    /**
     * Dispatches named function calls (contains, startsWith, size, int, string, etc.)
     */
    private void visitCallFunc(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();

        switch (fun) {
            case CONTAINS -> callContains(expr);
            case STARTS_WITH -> callStartsWith(expr);
            case ENDS_WITH -> callEndsWith(expr);
            case MATCHES -> callMatches(expr);
            case SIZE -> callSize(expr);
            case LOWER_ASCII -> callLowerAscii(expr);
            case UPPER_ASCII -> callUpperAscii(expr);
            case TRIM -> callTrim(expr);
            case CHAR_AT -> callCharAt(expr);
            case INDEX_OF -> callIndexOf(expr);
            case LAST_INDEX_OF -> callLastIndexOf(expr);
            case SUBSTRING -> callSubstring(expr);
            case REPLACE -> callReplace(expr);
            case REVERSE -> callReverse(expr);
            case SPLIT -> callSplit(expr);
            case JOIN -> callJoin(expr);
            case TYPE_CONVERT_BOOL, TYPE_CONVERT_BYTES, TYPE_CONVERT_DOUBLE,
                 TYPE_CONVERT_INT, TYPE_CONVERT_STRING, TYPE_CONVERT_UINT -> callCasting(expr);
            case TYPE_CONVERT_DURATION -> callDuration(expr);
            case TYPE_CONVERT_TIMESTAMP -> callTimestampFromString(expr);
            case TIME_GET_FULL_YEAR, TIME_GET_MONTH, TIME_GET_DATE,
                 TIME_GET_HOURS, TIME_GET_MINUTES, TIME_GET_SECONDS,
                 TIME_GET_MILLISECONDS, TIME_GET_DAY_OF_YEAR,
                 TIME_GET_DAY_OF_MONTH, TIME_GET_DAY_OF_WEEK -> callExtractFromTimestamp(expr);
            default -> throw new ConversionException(
                    ErrorMessages.UNSUPPORTED_EXPRESSION,
                    "unsupported function: " + fun);
        }
    }

    // ========================================================================
    // String Functions
    // ========================================================================

    /**
     * Converts str.contains(substr) to POSITION(substr IN str) > 0.
     */
    private void callContains(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "contains() requires a target"));
        CelExpr needle = call.args().get(0);

        dialect.writeContains(str,
                () -> visit(target),
                () -> visit(needle));
    }

    /**
     * Converts str.startsWith(prefix) to str LIKE 'prefix%' ESCAPE.
     */
    private void callStartsWith(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "startsWith() requires a target"));
        CelExpr prefix = call.args().get(0);

        if (isStringLiteral(prefix)) {
            String pattern = escapeLikePattern(prefix.constant().stringValue());
            visit(target);
            str.append(" LIKE '").append(pattern).append("%'");
            dialect.writeLikeEscape(str);
        } else {
            // Dynamic pattern: use POSITION
            str.append("POSITION(");
            visit(prefix);
            str.append(" IN ");
            visit(target);
            str.append(") = 1");
        }
    }

    /**
     * Converts str.endsWith(suffix) to str LIKE '%suffix' ESCAPE.
     */
    private void callEndsWith(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "endsWith() requires a target"));
        CelExpr suffix = call.args().get(0);

        if (isStringLiteral(suffix)) {
            String pattern = escapeLikePattern(suffix.constant().stringValue());
            visit(target);
            str.append(" LIKE '%").append(pattern).append("'");
            dialect.writeLikeEscape(str);
        } else {
            // Dynamic pattern: use RIGHT() comparison
            str.append("RIGHT(");
            visit(target);
            str.append(", LENGTH(");
            visit(suffix);
            str.append(")) = ");
            visit(suffix);
        }
    }

    /**
     * Converts str.matches(regex) to dialect-specific regex match.
     */
    private void callMatches(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "matches() requires a target"));
        CelExpr pattern = call.args().get(0);

        if (!dialect.supportsRegex()) {
            throw new ConversionException(
                    ErrorMessages.UNSUPPORTED_EXPRESSION,
                    "regex matching is not supported by dialect " + dialect.name());
        }

        if (isStringLiteral(pattern)) {
            String re2Pattern = pattern.constant().stringValue();
            RegexResult result = dialect.convertRegex(re2Pattern);
            dialect.writeRegexMatch(str, () -> visit(target), result.pattern(), result.caseInsensitive());
        } else {
            // Dynamic regex: use ~ operator directly
            visit(target);
            str.append(" ~ ");
            visit(pattern);
        }
    }

    /**
     * Converts size(x) or x.size() to LENGTH(x) for strings or ARRAY_LENGTH for arrays.
     */
    private void callSize(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target;
        if (call.target().isPresent()) {
            target = call.target().get();
        } else if (!call.args().isEmpty()) {
            target = call.args().get(0);
        } else {
            throw new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "size() requires an argument");
        }

        CelType targetType = getType(target);

        // JSON array size
        if (shouldUseJSONPath(target)) {
            TableAndField tf = getTableAndFieldFromSelectChain(target);
            if (tf != null) {
                FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
                if (fieldSchema != null && (fieldSchema.isJSON() || fieldSchema.isJSONB())) {
                    if (isJSONArrayField(target)) {
                        dialect.writeJSONArrayLength(str, () -> visit(target));
                        return;
                    }
                }
            }
        }

        // Array size
        if (isListType(targetType)) {
            int dimension = getArrayDimension(target);
            dialect.writeArrayLength(str, dimension, () -> visit(target));
            return;
        }

        // Map size
        if (isMapType(targetType)) {
            // For maps/JSON objects: use json_object_keys count
            str.append("(SELECT COUNT(*) FROM jsonb_object_keys(");
            visit(target);
            str.append("))");
            return;
        }

        // String length
        str.append("LENGTH(");
        visit(target);
        str.append(')');
    }

    /**
     * Converts str.lowerAscii() to LOWER(str).
     */
    private void callLowerAscii(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "lowerAscii() requires a target"));
        str.append("LOWER(");
        visit(target);
        str.append(')');
    }

    /**
     * Converts str.upperAscii() to UPPER(str).
     */
    private void callUpperAscii(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "upperAscii() requires a target"));
        str.append("UPPER(");
        visit(target);
        str.append(')');
    }

    /**
     * Converts str.trim() to TRIM(str).
     */
    private void callTrim(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "trim() requires a target"));
        str.append("TRIM(");
        visit(target);
        str.append(')');
    }

    /**
     * Converts str.charAt(idx) to SUBSTRING(str, idx+1, 1).
     */
    private void callCharAt(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "charAt() requires a target"));
        CelExpr index = call.args().get(0);

        str.append("SUBSTRING(");
        visit(target);
        str.append(", ");
        if (index.getKind() == Kind.CONSTANT && index.constant().getKind() == CelConstant.Kind.INT64_VALUE) {
            str.append(index.constant().int64Value() + 1);
        } else {
            visit(index);
            str.append(" + 1");
        }
        str.append(", 1)");
    }

    /**
     * Converts str.indexOf(substr) to a CASE WHEN POSITION expression.
     * Returns -1 if not found, otherwise the 0-based index.
     */
    private void callIndexOf(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "indexOf() requires a target"));
        CelExpr needle = call.args().get(0);

        if (call.args().size() == 1) {
            // indexOf(substr) with no offset
            str.append("CASE WHEN POSITION(");
            visit(needle);
            str.append(" IN ");
            visit(target);
            str.append(") > 0 THEN POSITION(");
            visit(needle);
            str.append(" IN ");
            visit(target);
            str.append(") - 1 ELSE -1 END");
        } else {
            // indexOf(substr, offset)
            CelExpr offset = call.args().get(1);
            str.append("CASE WHEN POSITION(");
            visit(needle);
            str.append(" IN SUBSTRING(");
            visit(target);
            str.append(", ");
            visit(offset);
            str.append(" + 1)) > 0 THEN POSITION(");
            visit(needle);
            str.append(" IN SUBSTRING(");
            visit(target);
            str.append(", ");
            visit(offset);
            str.append(" + 1)) + ");
            visit(offset);
            str.append(" - 1 ELSE -1 END");
        }
    }

    /**
     * Converts str.lastIndexOf(substr) using REVERSE and POSITION.
     */
    private void callLastIndexOf(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "lastIndexOf() requires a target"));
        CelExpr needle = call.args().get(0);

        if (call.args().size() == 1) {
            // lastIndexOf(substr) with no offset
            str.append("CASE WHEN POSITION(REVERSE(");
            visit(needle);
            str.append(") IN REVERSE(");
            visit(target);
            str.append(")) > 0 THEN LENGTH(");
            visit(target);
            str.append(") - POSITION(REVERSE(");
            visit(needle);
            str.append(") IN REVERSE(");
            visit(target);
            str.append(")) - LENGTH(");
            visit(needle);
            str.append(") + 1 ELSE -1 END");
        } else {
            // lastIndexOf(substr, offset) - search only in target[0:offset]
            CelExpr offset = call.args().get(1);
            str.append("CASE WHEN POSITION(REVERSE(");
            visit(needle);
            str.append(") IN REVERSE(SUBSTRING(");
            visit(target);
            str.append(", 1, ");
            visit(offset);
            str.append("))) > 0 THEN ");
            visit(offset);
            str.append(" - POSITION(REVERSE(");
            visit(needle);
            str.append(") IN REVERSE(SUBSTRING(");
            visit(target);
            str.append(", 1, ");
            visit(offset);
            str.append("))) - LENGTH(");
            visit(needle);
            str.append(") + 1 ELSE -1 END");
        }
    }

    /**
     * Converts str.substring(start) or str.substring(start, end) to SUBSTRING.
     * CEL uses 0-based indexing; SQL uses 1-based.
     */
    private void callSubstring(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "substring() requires a target"));
        CelExpr start = call.args().get(0);

        str.append("SUBSTRING(");
        visit(target);
        str.append(", ");

        if (start.getKind() == Kind.CONSTANT && start.constant().getKind() == CelConstant.Kind.INT64_VALUE) {
            str.append(start.constant().int64Value() + 1);
        } else {
            visit(start);
            str.append(" + 1");
        }

        if (call.args().size() > 1) {
            CelExpr end = call.args().get(1);
            str.append(", ");
            // Length = end - start
            if (start.getKind() == Kind.CONSTANT && start.constant().getKind() == CelConstant.Kind.INT64_VALUE
                    && end.getKind() == Kind.CONSTANT && end.constant().getKind() == CelConstant.Kind.INT64_VALUE) {
                str.append(end.constant().int64Value() - start.constant().int64Value());
            } else {
                visit(end);
                str.append(" - ");
                visit(start);
            }
        }

        str.append(')');
    }

    /**
     * Converts str.replace(old, new) to REPLACE(str, old, new).
     */
    private void callReplace(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "replace() requires a target"));

        if (call.args().size() < 2) {
            throw new ConversionException(ErrorMessages.INVALID_ARGUMENTS,
                    "replace() requires two arguments (old, new)");
        }

        CelExpr oldStr = call.args().get(0);
        CelExpr newStr = call.args().get(1);

        // Check for optional maxReplacements argument
        if (call.args().size() > 2) {
            CelExpr maxReplacements = call.args().get(2);
            if (maxReplacements.getKind() == Kind.CONSTANT
                    && maxReplacements.constant().getKind() == CelConstant.Kind.INT64_VALUE
                    && maxReplacements.constant().int64Value() == -1) {
                // -1 means replace all, which is the default
                str.append("REPLACE(");
                visit(target);
                str.append(", ");
                visit(oldStr);
                str.append(", ");
                visit(newStr);
                str.append(')');
                return;
            }
            // Limited replacement not easily supported in SQL, fall through to REPLACE
        }

        str.append("REPLACE(");
        visit(target);
        str.append(", ");
        visit(oldStr);
        str.append(", ");
        visit(newStr);
        str.append(')');
    }

    /**
     * Converts str.reverse() to REVERSE(str).
     */
    private void callReverse(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target;
        if (call.target().isPresent()) {
            target = call.target().get();
        } else if (!call.args().isEmpty()) {
            target = call.args().get(0);
        } else {
            throw new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "reverse() requires a target");
        }

        str.append("REVERSE(");
        visit(target);
        str.append(')');
    }

    /**
     * Converts str.split(delim) to STRING_TO_ARRAY(str, delim).
     * With limit: (STRING_TO_ARRAY(str, delim))[1:limit]
     */
    private void callSplit(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "split() requires a target"));
        CelExpr delim = call.args().get(0);

        if (call.args().size() > 1) {
            CelExpr limit = call.args().get(1);
            if (limit.getKind() == Kind.CONSTANT && limit.constant().getKind() == CelConstant.Kind.INT64_VALUE) {
                long limitVal = limit.constant().int64Value();
                dialect.writeSplitWithLimit(str,
                        () -> visit(target),
                        () -> visit(delim),
                        limitVal);
                return;
            }
            // Dynamic limit: just use split without limit
        }

        dialect.writeSplit(str,
                () -> visit(target),
                () -> visit(delim));
    }

    /**
     * Converts list.join() or list.join(delim) to ARRAY_TO_STRING.
     */
    private void callJoin(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "join() requires a target"));

        if (call.args().isEmpty()) {
            dialect.writeJoin(str, () -> visit(target), null);
        } else {
            CelExpr delim = call.args().get(0);
            dialect.writeJoin(str, () -> visit(target), () -> visit(delim));
        }
    }

    // ========================================================================
    // Type Casting Functions
    // ========================================================================

    /**
     * Converts int(x), string(x), double(x), etc. to CAST(x AS TYPE).
     * Special handling for int(timestamp) -> EXTRACT(EPOCH FROM x).
     */
    private void callCasting(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();
        CelExpr arg = call.args().get(0);

        // Special case: int(timestamp) -> extract epoch
        if (TYPE_CONVERT_INT.equals(fun) || TYPE_CONVERT_UINT.equals(fun)) {
            CelType argType = getType(arg);
            if (isTimestampType(argType)) {
                dialect.writeEpochExtract(str, () -> visit(arg));
                return;
            }
        }

        str.append("CAST(");
        visit(arg);
        str.append(" AS ");
        dialect.writeTypeName(str, fun);
        str.append(')');
    }

    // ========================================================================
    // Duration Functions
    // ========================================================================

    /**
     * Converts duration("10s") to INTERVAL 10 SECOND.
     * Parses Go-style duration strings: h, m, s, ms, us, ns.
     */
    private void callDuration(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr arg = call.args().get(0);

        if (!isStringLiteral(arg)) {
            throw new ConversionException(ErrorMessages.INVALID_DURATION,
                    "duration() requires a string literal argument");
        }

        String durStr = arg.constant().stringValue();
        DurationComponents dc = parseDurationString(durStr);

        dialect.writeDuration(str, dc.value, dc.unit);
    }

    /**
     * Represents parsed duration components.
     */
    private record DurationComponents(long value, String unit) {}

    /**
     * Parses a Go-style duration string like "10s", "1h30m", "500ms".
     * Converts to the largest unit that divides evenly.
     */
    private DurationComponents parseDurationString(String durStr) throws ConversionException {
        if (durStr == null || durStr.isEmpty()) {
            throw new ConversionException(ErrorMessages.INVALID_DURATION,
                    "empty duration string");
        }

        // Handle negative durations
        boolean negative = false;
        String input = durStr;
        if (input.startsWith("-")) {
            negative = true;
            input = input.substring(1);
        }

        long totalNanos = 0;
        Matcher matcher = DURATION_UNIT_PATTERN.matcher(input);
        boolean found = false;

        while (matcher.find()) {
            found = true;
            long val = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2);
            switch (unit) {
                case "h" -> totalNanos += val * 3_600_000_000_000L;
                case "m" -> totalNanos += val * 60_000_000_000L;
                case "s" -> totalNanos += val * 1_000_000_000L;
                case "ms" -> totalNanos += val * 1_000_000L;
                case "us" -> totalNanos += val * 1_000L;
                case "ns" -> totalNanos += val;
            }
        }

        if (!found) {
            throw new ConversionException(ErrorMessages.INVALID_DURATION,
                    "cannot parse duration: " + durStr);
        }

        if (negative) {
            totalNanos = -totalNanos;
        }

        // Find the best unit
        if (totalNanos % 3_600_000_000_000L == 0) {
            return new DurationComponents(totalNanos / 3_600_000_000_000L, "HOUR");
        }
        if (totalNanos % 60_000_000_000L == 0) {
            return new DurationComponents(totalNanos / 60_000_000_000L, "MINUTE");
        }
        if (totalNanos % 1_000_000_000L == 0) {
            return new DurationComponents(totalNanos / 1_000_000_000L, "SECOND");
        }
        if (totalNanos % 1_000_000L == 0) {
            return new DurationComponents(totalNanos / 1_000_000L, "MILLISECOND");
        }
        if (totalNanos % 1_000L == 0) {
            return new DurationComponents(totalNanos / 1_000L, "MICROSECOND");
        }

        // Fallback to seconds with fractional
        double seconds = totalNanos / 1_000_000_000.0;
        return new DurationComponents((long) seconds, "SECOND");
    }

    // ========================================================================
    // Timestamp Functions
    // ========================================================================

    /**
     * Converts timestamp("2024-01-01T00:00:00Z") to CAST(expr AS TIMESTAMP WITH TIME ZONE).
     */
    private void callTimestampFromString(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        CelExpr arg = call.args().get(0);

        dialect.writeTimestampCast(str, () -> visit(arg));
    }

    /**
     * Converts timestamp +/- duration to dialect timestamp arithmetic.
     */
    private void callTimestampOperation(CelExpr timestamp, CelExpr duration, String op) throws ConversionException {
        dialect.writeTimestampArithmetic(str, op,
                () -> visit(timestamp),
                () -> visit(duration));
    }

    /**
     * Converts getFullYear(), getMonth(), etc. to EXTRACT(PART FROM expr).
     */
    private void callExtractFromTimestamp(CelExpr expr) throws ConversionException {
        CelCall call = expr.call();
        String fun = call.function();
        CelExpr target = call.target().orElseThrow(() ->
                new ConversionException(ErrorMessages.INVALID_ARGUMENTS,
                        fun + "() requires a target"));

        String part = mapTimeFunctionToPart(fun);

        // Check for timezone argument
        if (!call.args().isEmpty()) {
            CelExpr tz = call.args().get(0);
            dialect.writeExtract(str, part, () -> visit(target), () -> visit(tz));
        } else {
            dialect.writeExtract(str, part, () -> visit(target), null);
        }
    }

    /**
     * Maps CEL time function names to SQL EXTRACT parts.
     */
    private String mapTimeFunctionToPart(String fun) throws ConversionException {
        return switch (fun) {
            case TIME_GET_FULL_YEAR -> "YEAR";
            case TIME_GET_MONTH -> "MONTH";
            case TIME_GET_DATE, TIME_GET_DAY_OF_MONTH -> "DAY";
            case TIME_GET_HOURS -> "HOUR";
            case TIME_GET_MINUTES -> "MINUTE";
            case TIME_GET_SECONDS -> "SECOND";
            case TIME_GET_MILLISECONDS -> "MILLISECOND";
            case TIME_GET_DAY_OF_YEAR -> "DOY";
            case TIME_GET_DAY_OF_WEEK -> "DOW";
            default -> throw new ConversionException(
                    ErrorMessages.INVALID_TIMESTAMP_OP,
                    "unsupported time function: " + fun);
        };
    }

    // ========================================================================
    // visitList - List (Array) Literals
    // ========================================================================

    /**
     * Converts a CEL list literal [a, b, c] to SQL ARRAY[a, b, c].
     */
    private void visitList(CelExpr expr) throws ConversionException {
        CelList list = expr.list();
        ImmutableList<CelExpr> elements = list.elements();

        if (elements.isEmpty()) {
            // Empty list: need to check type for typed empty array
            CelType type = getType(expr);
            if (isListType(type) && type instanceof ListType lt && lt.hasElemType()) {
                String elemTypeName = celTypeToSqlTypeName(lt.elemType());
                dialect.writeEmptyTypedArray(str, elemTypeName);
            } else {
                dialect.writeArrayLiteralOpen(str);
                dialect.writeArrayLiteralClose(str);
            }
            return;
        }

        dialect.writeArrayLiteralOpen(str);
        for (int i = 0; i < elements.size(); i++) {
            if (i > 0) {
                str.append(", ");
            }
            visit(elements.get(i));
        }
        dialect.writeArrayLiteralClose(str);
    }

    /**
     * Maps a CelType to a SQL type name for empty array creation.
     */
    private String celTypeToSqlTypeName(CelType type) {
        if (type == null) return "TEXT";
        return switch (type.kind()) {
            case BOOL -> "BOOLEAN";
            case INT -> "BIGINT";
            case UINT -> "BIGINT";
            case DOUBLE -> "DOUBLE PRECISION";
            case STRING -> "TEXT";
            case BYTES -> "BYTEA";
            case TIMESTAMP -> "TIMESTAMP WITH TIME ZONE";
            default -> "TEXT";
        };
    }

    // ========================================================================
    // visitStruct - Struct (Message) Literals
    // ========================================================================

    /**
     * Converts a struct/message literal to SQL ROW(...).
     */
    private void visitStruct(CelExpr expr) throws ConversionException {
        CelStruct s = expr.struct();
        visitStructMsg(s);
    }

    /**
     * Writes a struct message as SQL ROW(field1, field2, ...).
     */
    private void visitStructMsg(CelStruct s) throws ConversionException {
        dialect.writeStructOpen(str);
        ImmutableList<CelStruct.Entry> entries = s.entries();
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) {
                str.append(", ");
            }
            visit(entries.get(i).value());
        }
        dialect.writeStructClose(str);
    }

    // ========================================================================
    // visitStructMap - Map Literals
    // ========================================================================

    /**
     * Converts a map literal {k1: v1, k2: v2} to a SQL expression.
     * For PostgreSQL, this generates jsonb_build_object(k1, v1, k2, v2).
     */
    private void visitStructMap(CelExpr expr) throws ConversionException {
        CelMap m = expr.map();
        ImmutableList<CelMap.Entry> entries = m.entries();

        if (entries.isEmpty()) {
            str.append("'{}'::jsonb");
            return;
        }

        str.append("jsonb_build_object(");
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) {
                str.append(", ");
            }
            visit(entries.get(i).key());
            str.append(", ");
            visit(entries.get(i).value());
        }
        str.append(')');
    }

    // ========================================================================
    // visitComprehension - Comprehensions (all, exists, exists_one, map, filter)
    // ========================================================================

    /**
     * Visits a comprehension expression. Identifies the comprehension pattern and
     * dispatches to the appropriate handler.
     */
    private void visitComprehension(CelExpr expr) throws ConversionException {
        comprehensionDepth++;
        if (comprehensionDepth > MAX_COMPREHENSION_DEPTH) {
            comprehensionDepth--;
            throw new ConversionException(
                    ErrorMessages.COMPREHENSION_DEPTH_EXCEEDED,
                    "comprehension nesting depth " + comprehensionDepth + " exceeds maximum of " + MAX_COMPREHENSION_DEPTH);
        }

        try {
            CelComprehension comp = expr.comprehension();
            ComprehensionKind kind = identifyComprehension(comp);

            switch (kind) {
                case ALL -> visitComprehensionAll(comp);
                case EXISTS -> visitComprehensionExists(comp);
                case EXISTS_ONE -> visitComprehensionExistsOne(comp);
                case MAP -> visitComprehensionMap(comp);
                case FILTER -> visitComprehensionFilter(comp);
                default -> throw new ConversionException(
                        ErrorMessages.UNSUPPORTED_COMPREHENSION,
                        "unsupported comprehension pattern");
            }
        } finally {
            comprehensionDepth--;
        }
    }

    /**
     * Enumeration of comprehension kinds.
     */
    private enum ComprehensionKind {
        ALL, EXISTS, EXISTS_ONE, MAP, FILTER, UNKNOWN
    }

    /**
     * Identifies the type of comprehension by examining its structure.
     * Based on the Go cel2sql comprehension pattern matching.
     */
    private ComprehensionKind identifyComprehension(CelComprehension comp) {
        // all: accuInit = true, loopCondition = @not_strictly_false(accuVar) && step, result = accuVar
        // exists: accuInit = false, loopCondition = @not_strictly_false(!accuVar) && step, result = accuVar
        // exists_one: accuInit = 0, step = conditional adding, result = accuVar == 1
        // map: accuInit = [], step = accuVar + [elem], result = accuVar
        // filter: accuInit = [], step = conditional append, result = accuVar

        CelExpr accuInit = comp.accuInit();
        CelExpr result = comp.result();
        CelExpr loopStep = comp.loopStep();

        // Check for all(): accuInit = true, result = accuVar
        if (isBoolTrue(accuInit) && isAccuVarRef(result, comp.accuVar())) {
            return ComprehensionKind.ALL;
        }

        // Check for exists(): accuInit = false, result = accuVar
        if (isBoolFalse(accuInit) && isAccuVarRef(result, comp.accuVar())) {
            return ComprehensionKind.EXISTS;
        }

        // Check for exists_one(): accuInit = 0, result = accuVar == 1
        if (isIntZero(accuInit) && isAccuEqualsOne(result, comp.accuVar())) {
            return ComprehensionKind.EXISTS_ONE;
        }

        // Check for map() or filter(): accuInit = []
        if (isEmptyList(accuInit)) {
            if (isMapStep(loopStep, comp.accuVar())) {
                return ComprehensionKind.MAP;
            }
            if (isFilterStep(loopStep, comp.accuVar())) {
                return ComprehensionKind.FILTER;
            }
            // Could also be a map without transform (just the element)
            return ComprehensionKind.MAP;
        }

        return ComprehensionKind.UNKNOWN;
    }

    /**
     * Checks if an expression is a reference to the accumulator variable.
     */
    private boolean isAccuVarRef(CelExpr expr, String accuVar) {
        return expr.getKind() == Kind.IDENT && expr.ident().name().equals(accuVar);
    }

    /**
     * Checks if result == (accuVar == 1) for exists_one detection.
     */
    private boolean isAccuEqualsOne(CelExpr expr, String accuVar) {
        if (expr.getKind() != Kind.CALL) return false;
        CelCall call = expr.call();
        if (!EQUALS.equals(call.function())) return false;
        if (call.args().size() != 2) return false;
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);
        return isAccuVarRef(lhs, accuVar)
                && rhs.getKind() == Kind.CONSTANT
                && rhs.constant().getKind() == CelConstant.Kind.INT64_VALUE
                && rhs.constant().int64Value() == 1;
    }

    /**
     * Checks if the loop step represents a map comprehension pattern.
     * Map step: accuVar + [transform_expr] (unconditional)
     */
    private boolean isMapStep(CelExpr step, String accuVar) {
        if (step.getKind() != Kind.CALL) return false;
        CelCall call = step.call();
        if (!ADD.equals(call.function())) return false;
        if (call.args().size() != 2) return false;
        CelExpr lhs = call.args().get(0);
        CelExpr rhs = call.args().get(1);
        return isAccuVarRef(lhs, accuVar) && rhs.getKind() == Kind.LIST;
    }

    /**
     * Checks if the loop step represents a filter comprehension pattern.
     * Filter step: conditional(predicate, accuVar + [elem], accuVar)
     */
    private boolean isFilterStep(CelExpr step, String accuVar) {
        if (step.getKind() != Kind.CALL) return false;
        CelCall call = step.call();
        if (!CONDITIONAL.equals(call.function())) return false;
        if (call.args().size() != 3) return false;
        CelExpr trueExpr = call.args().get(1);
        CelExpr falseExpr = call.args().get(2);
        return isAccuVarRef(falseExpr, accuVar)
                && trueExpr.getKind() == Kind.CALL
                && ADD.equals(trueExpr.call().function());
    }

    // ========================================================================
    // Comprehension Visitors
    // ========================================================================

    /**
     * Converts list.all(x, predicate) to:
     * NOT EXISTS (SELECT 1 FROM UNNEST(list) AS x WHERE NOT (predicate))
     */
    private void visitComprehensionAll(CelComprehension comp) throws ConversionException {
        CelExpr iterRange = comp.iterRange();
        String iterVar = comp.iterVar();
        CelExpr predicate = extractComprehensionPredicate(comp.loopCondition(), comp.loopStep());

        str.append("NOT EXISTS (SELECT 1 FROM ");
        dialect.writeComprehensionSource(str, () -> visit(iterRange), iterVar);
        str.append(" WHERE NOT (");
        visit(predicate);
        str.append("))");
    }

    /**
     * Converts list.exists(x, predicate) to:
     * EXISTS (SELECT 1 FROM UNNEST(list) AS x WHERE predicate)
     */
    private void visitComprehensionExists(CelComprehension comp) throws ConversionException {
        CelExpr iterRange = comp.iterRange();
        String iterVar = comp.iterVar();
        CelExpr predicate = extractComprehensionPredicate(comp.loopCondition(), comp.loopStep());

        str.append("EXISTS (SELECT 1 FROM ");
        dialect.writeComprehensionSource(str, () -> visit(iterRange), iterVar);
        str.append(" WHERE ");
        visit(predicate);
        str.append(')');
    }

    /**
     * Converts list.exists_one(x, predicate) to:
     * (SELECT COUNT(*) FROM UNNEST(list) AS x WHERE predicate) = 1
     */
    private void visitComprehensionExistsOne(CelComprehension comp) throws ConversionException {
        CelExpr iterRange = comp.iterRange();
        String iterVar = comp.iterVar();
        CelExpr predicate = extractExistsOnePredicate(comp.loopStep());

        str.append("(SELECT COUNT(*) FROM ");
        dialect.writeComprehensionSource(str, () -> visit(iterRange), iterVar);
        str.append(" WHERE ");
        visit(predicate);
        str.append(") = 1");
    }

    /**
     * Converts list.map(x, transform) to:
     * ARRAY(SELECT transform FROM UNNEST(list) AS x)
     */
    private void visitComprehensionMap(CelComprehension comp) throws ConversionException {
        CelExpr iterRange = comp.iterRange();
        String iterVar = comp.iterVar();
        CelExpr transform = extractMapTransform(comp.loopStep(), comp.accuVar());

        dialect.writeArraySubqueryOpen(str);
        visit(transform);
        dialect.writeArraySubqueryExprClose(str);
        str.append(" FROM ");
        dialect.writeComprehensionSource(str, () -> visit(iterRange), iterVar);
        str.append(')');
    }

    /**
     * Converts list.filter(x, predicate) to:
     * ARRAY(SELECT x FROM UNNEST(list) AS x WHERE predicate)
     */
    private void visitComprehensionFilter(CelComprehension comp) throws ConversionException {
        CelExpr iterRange = comp.iterRange();
        String iterVar = comp.iterVar();
        CelExpr predicate = extractFilterPredicate(comp.loopStep());

        dialect.writeArraySubqueryOpen(str);
        str.append(iterVar);
        dialect.writeArraySubqueryExprClose(str);
        str.append(" FROM ");
        dialect.writeComprehensionSource(str, () -> visit(iterRange), iterVar);
        str.append(" WHERE ");
        visit(predicate);
        str.append(')');
    }

    // ========================================================================
    // Comprehension Predicate Extraction
    // ========================================================================

    /**
     * Extracts the predicate from an all/exists comprehension.
     * The loopCondition is typically: @not_strictly_false(accuVar) && step (for all)
     * or @not_strictly_false(!accuVar) && step (for exists).
     * The actual predicate is in the loopStep.
     */
    private CelExpr extractComprehensionPredicate(CelExpr loopCondition, CelExpr loopStep) {
        // For all: step is the predicate (AND'd with current accu)
        // For exists: step is the predicate (OR'd with current accu)
        // The loopStep is typically: accuVar && predicate (all) or accuVar || predicate (exists)
        if (loopStep.getKind() == Kind.CALL) {
            CelCall call = loopStep.call();
            if (LOGICAL_OR.equals(call.function()) || LOGICAL_AND.equals(call.function())) {
                // The predicate is the second argument (first is the accu var ref)
                if (call.args().size() == 2) {
                    CelExpr second = call.args().get(1);
                    return second;
                }
            }
        }
        return loopStep;
    }

    /**
     * Extracts the predicate from an exists_one comprehension step.
     * Step is: conditional(predicate, accuVar + 1, accuVar)
     */
    private CelExpr extractExistsOnePredicate(CelExpr loopStep) {
        if (loopStep.getKind() == Kind.CALL) {
            CelCall call = loopStep.call();
            if (CONDITIONAL.equals(call.function()) && call.args().size() == 3) {
                return call.args().get(0); // The condition of the ternary
            }
        }
        return loopStep;
    }

    /**
     * Extracts the transform expression from a map comprehension step.
     * Step is: accuVar + [transform_expr]
     */
    private CelExpr extractMapTransform(CelExpr loopStep, String accuVar) {
        if (loopStep.getKind() == Kind.CALL) {
            CelCall call = loopStep.call();
            if (ADD.equals(call.function()) && call.args().size() == 2) {
                CelExpr rhs = call.args().get(1);
                if (rhs.getKind() == Kind.LIST) {
                    CelList list = rhs.list();
                    if (!list.elements().isEmpty()) {
                        return list.elements().get(0);
                    }
                }
            }
            // Filter step wrapped in conditional
            if (CONDITIONAL.equals(call.function()) && call.args().size() == 3) {
                CelExpr trueExpr = call.args().get(1);
                return extractMapTransform(trueExpr, accuVar);
            }
        }
        return loopStep;
    }

    /**
     * Extracts the predicate from a filter comprehension step.
     * Step is: conditional(predicate, accuVar + [iterVar], accuVar)
     */
    private CelExpr extractFilterPredicate(CelExpr loopStep) {
        if (loopStep.getKind() == Kind.CALL) {
            CelCall call = loopStep.call();
            if (CONDITIONAL.equals(call.function()) && call.args().size() == 3) {
                return call.args().get(0);
            }
        }
        return loopStep;
    }

    // ========================================================================
    // JSON Helpers
    // ========================================================================

    /**
     * Determines if a select expression should use JSON path access.
     * Returns true if the expression accesses a field declared as JSON/JSONB in the schema.
     */
    private boolean shouldUseJSONPath(CelExpr expr) {
        if (schemas == null || schemas.isEmpty()) return false;
        if (expr.getKind() != Kind.SELECT) return false;

        // Walk up the chain to find the root field
        TableAndField tf = getTableAndFieldFromSelectChain(expr);
        if (tf == null) return false;

        FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
        return fieldSchema != null && fieldSchema.isJSON();
    }

    /**
     * Checks if the expression chain contains a JSON field access.
     */
    private boolean hasJSONFieldInChain(CelExpr expr) {
        if (expr.getKind() != Kind.SELECT) return false;
        if (shouldUseJSONPath(expr)) return true;
        return hasJSONFieldInChain(expr.select().operand());
    }

    /**
     * Checks if the JSON extraction is the final (text) extraction.
     * Used to determine whether to use -> (JSON) or ->> (text) operator.
     */
    private boolean isJSONTextExtraction(CelExpr expr) {
        // If this expression's result is used in a comparison or string context, extract as text
        // For now, the default is to extract as text (->>) for the final access
        // A non-final access (intermediate) uses -> to preserve JSON type
        CelType type = getType(expr);
        return type != null && type.kind() != CelKind.MAP && type.kind() != CelKind.LIST;
    }

    /**
     * Checks if a field in a select chain is a JSON array field.
     */
    private boolean isJSONArrayField(CelExpr expr) {
        TableAndField tf = getTableAndFieldFromSelectChain(expr);
        if (tf == null) return false;
        FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
        return fieldSchema != null && (fieldSchema.isJSON() || fieldSchema.isJSONB()) && fieldSchema.repeated();
    }

    /**
     * Checks if an expression is a JSON object field access.
     */
    private boolean isJSONObjectFieldAccess(CelExpr expr) {
        if (expr.getKind() != Kind.SELECT) return false;
        return shouldUseJSONPath(expr.select().operand());
    }

    /**
     * Checks if an expression is a nested JSON access (operand is also a JSON select).
     */
    private boolean isNestedJSONAccess(CelExpr expr) {
        if (expr.getKind() != Kind.SELECT) return false;
        return shouldUseJSONPath(expr);
    }

    /**
     * Gets the JSON array function name based on the JSON type.
     */
    private String getJSONArrayFunction(boolean isJSONB, boolean asText) {
        if (isJSONB) {
            return asText ? "jsonb_array_elements_text" : "jsonb_array_elements";
        } else {
            return asText ? "json_array_elements_text" : "json_array_elements";
        }
    }

    /**
     * Builds a JSON path from a nested select chain.
     * Returns the root expression and populates the path list.
     */
    private CelExpr buildJSONPathInternal(CelExpr expr, List<String> path) {
        if (expr.getKind() != Kind.SELECT) return expr;
        CelSelect sel = expr.select();
        CelExpr operand = sel.operand();

        // Check if operand is the root JSON field
        if (!isNestedJSONAccess(operand)) {
            path.add(0, sel.field());
            return operand;
        }

        path.add(0, sel.field());
        return buildJSONPathInternal(operand, path);
    }

    // ========================================================================
    // Schema Helpers
    // ========================================================================

    /**
     * Extracts table and field name from a select chain like table.field or field.
     */
    private TableAndField getTableAndFieldFromSelectChain(CelExpr expr) {
        if (expr.getKind() == Kind.SELECT) {
            CelSelect sel = expr.select();
            CelExpr operand = sel.operand();

            // Check if operand is an ident (direct field access or table.field)
            if (operand.getKind() == Kind.IDENT) {
                String table = operand.ident().name();
                String field = sel.field();
                return new TableAndField(table, field);
            }

            // Walk up the chain to find the root
            if (operand.getKind() == Kind.SELECT) {
                return getTableAndFieldFromSelectChain(operand);
            }
        }

        // For ident expressions, there's no table prefix
        if (expr.getKind() == Kind.IDENT) {
            return new TableAndField("", expr.ident().name());
        }

        return null;
    }

    /**
     * Finds a field schema by table name and field name.
     */
    private FieldSchema findFieldSchema(String table, String field) {
        if (schemas == null) return null;

        // Try table-specific lookup first
        if (table != null && !table.isEmpty()) {
            Schema schema = schemas.get(table);
            if (schema != null) {
                Optional<FieldSchema> fs = schema.findField(field);
                if (fs.isPresent()) return fs.get();
            }
        }

        // Try all schemas
        for (Schema schema : schemas.values()) {
            Optional<FieldSchema> fs = schema.findField(field);
            if (fs.isPresent()) return fs.get();
        }

        return null;
    }

    /**
     * Gets the array dimension for a field.
     */
    private int getArrayDimension(CelExpr expr) {
        TableAndField tf = getTableAndFieldFromSelectChain(expr);
        if (tf != null) {
            FieldSchema fieldSchema = findFieldSchema(tf.table, tf.field);
            if (fieldSchema != null && fieldSchema.dimensions() > 0) {
                return fieldSchema.dimensions();
            }
        }
        return 1;
    }

    /**
     * Simple record to hold table and field names from a select chain.
     */
    private record TableAndField(String table, String field) {}

    // ========================================================================
    // Utility Helpers
    // ========================================================================

    /**
     * Escapes special characters in a LIKE pattern (%, _, \).
     */
    private String escapeLikePattern(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '%' -> sb.append("\\%");
                case '_' -> sb.append("\\_");
                case '\\' -> sb.append("\\\\");
                case '\'' -> sb.append("''");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Gets the string value from a string literal expression.
     */
    private String getStringValue(CelExpr expr) {
        if (isStringLiteral(expr)) {
            return expr.constant().stringValue();
        }
        return "";
    }

    // ========================================================================
    // Parenthesization / Precedence Helpers
    // ========================================================================

    /**
     * Visits a child expression, wrapping it in parentheses if needed based on
     * operator precedence.
     */
    private void visitMaybeNested(CelExpr parent, CelExpr child) throws ConversionException {
        boolean needsParens = isComplexOperatorWithRespectTo(parent, child);
        if (needsParens) {
            str.append('(');
        }
        visit(child);
        if (needsParens) {
            str.append(')');
        }
    }

    /**
     * Checks if the child expression is a complex operator that needs parenthesization
     * with respect to the parent expression.
     */
    private boolean isComplexOperatorWithRespectTo(CelExpr parent, CelExpr child) {
        if (!isComplexOperator(child)) return false;
        if (!isComplexOperator(parent)) return false;

        String parentOp = getOperator(parent);
        String childOp = getOperator(child);

        if (parentOp == null || childOp == null) return false;

        // If same precedence, check for left-recursiveness
        if (isSamePrecedence(parentOp, childOp)) {
            // For non-commutative operators, right-hand side needs parens
            return isLeftRecursive(parent, child);
        }

        // If child has lower precedence than parent, needs parens
        return isLowerPrecedence(childOp, parentOp);
    }

    /**
     * Checks if an expression is a complex operator (binary, unary, or ternary).
     */
    private boolean isComplexOperator(CelExpr expr) {
        if (expr.getKind() != Kind.CALL) return false;
        String op = expr.call().function();
        return isBinaryOrTernaryOperator(op) || LOGICAL_NOT.equals(op) || NEGATE.equals(op);
    }

    /**
     * Gets the operator name from a call expression.
     */
    private String getOperator(CelExpr expr) {
        if (expr.getKind() != Kind.CALL) return null;
        return expr.call().function();
    }

    /**
     * Checks if two operators have the same precedence.
     */
    private boolean isSamePrecedence(String op1, String op2) {
        return getPrecedence(op1) == getPrecedence(op2);
    }

    /**
     * Checks if op1 has lower precedence than op2.
     */
    private boolean isLowerPrecedence(String op1, String op2) {
        return getPrecedence(op1) > getPrecedence(op2);
    }

    /**
     * Gets the precedence level for an operator. Higher number = lower precedence (binds less tightly).
     */
    private int getPrecedence(String op) {
        return PRECEDENCE_MAP.getOrDefault(op, 0);
    }

    /**
     * Checks if a child expression is the right-hand side operand of a binary parent,
     * making it potentially need parenthesization for left-recursive operators.
     */
    private boolean isLeftRecursive(CelExpr parent, CelExpr child) {
        if (parent.getKind() != Kind.CALL) return false;
        CelCall call = parent.call();
        if (call.args().size() != 2) return false;
        // Child is left-recursive if it's the RHS of the parent
        return call.args().get(1).id() == child.id();
    }

    /**
     * Checks if an operator name represents a binary or ternary operator.
     */
    private boolean isBinaryOrTernaryOperator(String op) {
        return switch (op) {
            case CONDITIONAL, LOGICAL_AND, LOGICAL_OR, EQUALS, NOT_EQUALS,
                 LESS, LESS_EQUALS, GREATER, GREATER_EQUALS,
                 ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO,
                 IN, OLD_IN, INDEX -> true;
            default -> false;
        };
    }
}
