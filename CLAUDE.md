# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

cel2sql4j is a Java library that converts CEL (Common Expression Language) expressions into SQL WHERE clauses. It is a port of the Go library `github.com/spandigital/cel2sql/v3`. The Go source lives at `/Users/richardwooding/Code/SPAN/cel2sql` and can be used as a reference.

## Build & Test Commands

```bash
./gradlew build              # Full build (compile + test)
./gradlew compileJava        # Compile only
./gradlew test               # Run all tests
./gradlew test --rerun       # Force re-run (skip cache)

# Run a single test class
./gradlew test --tests "com.spandigital.cel2sql.Cel2SqlBasicTest"

# Run a single test method (use the display name or method name)
./gradlew test --tests "com.spandigital.cel2sql.Cel2SqlBasicTest.testBasicPostgres"
```

Gradle 8.12, Java 17 source/target (runs on any JDK 17+).

## Architecture

### Public API (`Cel2Sql.java`)

Three entry points, each with varargs-Consumer and pre-built-options overloads:
- `convert(ast, ...)` — inline literals in the SQL string
- `convertParameterized(ast, ...)` — placeholders + extracted parameter list
- `analyzeQuery(ast, ...)` — SQL + dialect-specific index recommendations

All default to PostgreSQL when no dialect is specified.

### Core Conversion (`Converter.java`)

Single-pass AST visitor (~1600 lines). Walks `CelExpr` nodes via `visit()` dispatch on `expr.getKind()` (CALL, COMPREHENSION, CONSTANT, IDENT, LIST, SELECT, STRUCT, MAP). Key internal concerns:

- **Operator precedence** — static `PRECEDENCE_MAP` drives parenthesization decisions
- **Parameterization** — booleans and nulls are always inlined (never parameterized) for query plan optimization
- **Depth/length guards** — `maxDepth` (default 100) and `maxOutputLength` (default 50000) prevent runaway queries
- **`SqlWriter` callback pattern** — dialect methods receive `SqlWriter` lambdas (closures that append to the shared `StringBuilder`) instead of pre-rendered strings, so dialect code controls output ordering

### Dialect System

`Dialect` interface (~35 methods) organized by: literals, operators, type casting, arrays, JSON, timestamps, string functions, comprehensions, structs, validation, regex, capabilities.

Each dialect package contains:
- `XxxDialect.java` — implements `Dialect` + `IndexAdvisor`
- `XxxValidation.java` — field name validation, reserved keyword set
- `XxxRegex.java` — RE2 → dialect-native regex conversion with ReDoS protection (except SQLite which doesn't support regex)

Key dialect differences to be aware of:
| Feature | PostgreSQL | MySQL | SQLite | DuckDB | BigQuery |
|---|---|---|---|---|---|
| Param style | `$N` | `?` | `?` | `$N` | `@pN` |
| String concat | `\|\|` | `CONCAT()` | `\|\|` | `\|\|` | `\|\|` |
| Array type | native | JSON | JSON | native | native |
| Contains | `POSITION()` | `LOCATE()` | `INSTR()` | `CONTAINS()` | `STRPOS()` |
| Regex | `~` / `~*` | `REGEXP` | unsupported | `~` / `~*` | `REGEXP_CONTAINS()` |

### Error Handling

`ConversionException` separates `userMessage` (safe for end users) from `internalDetails` (for logs only) — CWE-209 pattern. Factory methods: `ConversionException.of(userMsg, details)` and `ConversionException.wrap(userMsg, cause)`.

### Schema System

`FieldSchema` (record) and `Schema` describe table columns for JSON/JSONB field detection. Passed via `ConvertOptions.withSchemas()`. When schemas are absent, the converter treats all fields as plain columns.

## Test Patterns

Tests use JUnit 5 parameterized tests with `@MethodSource` providing `Stream<Arguments>`. The `CelHelper` utility class provides a `standardCompiler()` pre-loaded with common variable declarations (name:string, age:int, adult:bool, etc.) and `compile(celExpr)` to get a checked AST.

To add a new test case, add an `Arguments.of(testName, celExpression, expectedSql)` entry to the relevant `*Tests` method source stream.

## CEL Java API Essentials

The Java CEL library (`dev.cel`) differs from Go's `cel-go`:
- `CelExpr.getKind()` returns a `Kind` enum, not a protobuf oneof
- `ast.getType(expr.id())` returns `Optional<CelType>`
- Comprehension macros require `CelStandardMacro.STANDARD_MACROS` on the compiler builder
- `CelConstant.getKind()` returns a Kind enum for literal type dispatch
