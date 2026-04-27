---
name: add-cel-function
description: Adds a new CEL standard library function (or operator) to cel2sql4j across all six SQL dialects. Use when implementing a CEL function such as `trim`, `format`, `getMonth`, `lowerAscii`, or any new operator that requires per-dialect SQL output — the workflow touches `Converter.java` constants and dispatch, the `Dialect` interface, every dialect implementation (postgres, mysql, sqlite, duckdb, bigquery, spark), and the parameterized test class for that function category.
---

# Add a CEL Function

Implementing a new CEL function in cel2sql4j is mechanical but error-prone — easy to forget a dialect, miss a test class, or skip the type-checker registration that makes the test compile. This skill captures the file map and conventions so each new function lands the same way.

## File-touchpoint checklist

A new CEL function `xxx` typically requires changes in 4 areas. Work through them in this order:

### 1. Wire the dispatch in `Converter.java`

```java
// 1a. Add a constant alongside SPLIT, JOIN, FORMAT etc.
static final String XXX = "xxx";

// 1b. Add a case to visitCallFunc() (search for `case SPLIT -> callSplit(expr);`)
case XXX -> callXxx(expr);

// 1c. Add a private callXxx(CelExpr expr) method modelled after callJoin / callSplit / callFormat.
//     - Pull target / args
//     - Throw ConversionException with INVALID_ARGUMENTS for wrong arity
//     - Delegate emission to dialect.writeXxx(...)
```

### 2. Extend the `Dialect` interface

In `src/main/java/com/spandigital/cel2sql/dialect/Dialect.java`, add a new method in the appropriate section (string functions / timestamps / etc.). Document **what the implementation receives** — typically an unquoted/validated value plus `SqlWriter` callbacks for sub-expressions. The dialect is responsible for any quoting/escaping via `writeStringLiteral`. See [references/sql-writer-pattern.md](references/sql-writer-pattern.md) for the closure-callback convention.

### 3. Implement in all six dialects

Six files, in this order (Postgres first, Spark last so you don't forget any):

1. `dialect/postgres/PostgresDialect.java`
2. `dialect/mysql/MySqlDialect.java`
3. `dialect/sqlite/SqliteDialect.java`
4. `dialect/duckdb/DuckDbDialect.java`
5. `dialect/bigquery/BigQueryDialect.java`
6. `dialect/spark/SparkDialect.java`

If a dialect has no native equivalent, **throw rather than emit broken SQL**:

```java
throw ConversionException.of(
    "Unsupported operation",
    "<feature> is not supported in <Dialect>: <reason>");
```

Precedents: `MySqlDialect.writeFormat` (no printf in MySQL), `SparkDialect.writeJSONArrayMembership` (multi-row scalar subquery is invalid), `SqliteDialect.writeRegexMatch` (no regex). See [references/conversion-exception.md](references/conversion-exception.md) for the userMessage / internalDetails split.

### 4. Tests

- Find the right parameterized test class (`Cel2SqlStringTest` for string fns, `Cel2SqlTimestampTest` for time, `Cel2SqlArrayTest` for array, etc.) and add `Arguments.of(name, celExpr, "<DIALECT>", DIALECT, expectedSql)` lines for each dialect.
- For dialects that throw, add a separate `assertThatThrownBy(...)` test rather than mixing throw-cases into the parameterized stream.
- If the CEL function isn't part of `dev.cel.extensions.CelExtensions.strings()` (cel-java's strings extension only includes split/join/charAt/indexOf/replace/substring/trim/lower/upperAscii/lastIndexOf — no `format`), declare it manually in a custom test compiler. Pattern (verbatim from `Cel2SqlOptionsTest.stringsExtCompiler`):

```java
.addFunctionDeclarations(
    CelFunctionDecl.newFunctionDeclaration(
        "xxx",
        CelOverloadDecl.newMemberOverload(
            "string_xxx_list",                    // unique overload id
            SimpleType.STRING,                     // return type
            SimpleType.STRING,                     // receiver type
            ListType.create(SimpleType.DYN))))    // args
```

### 5. Run

```bash
./gradlew test --tests "com.spandigital.cel2sql.Cel2SqlStringTest"   # focused
./gradlew build                                                       # full
```

## Conventions

- **No comments unless WHY is non-obvious.** Don't write `// adds the constant XXX` — the code says that.
- **Don't log.** The `logger` field exists but is rarely used; rely on tests for diagnostics.
- **Match the existing `callXxx` shape.** Look at the closest existing method (string-vs-array-vs-timestamp) and follow its argument-extraction style and error-message wording.
- **Boolean and null literals stay inlined**, never parameterized — see `Converter.visitConst` for the precedent. New functions should not break this.

## References

- [references/sql-writer-pattern.md](references/sql-writer-pattern.md) — the `SqlWriter` closure-callback convention; why dialects receive lambdas instead of pre-rendered strings.
- [references/conversion-exception.md](references/conversion-exception.md) — the CWE-209 split between user-safe `userMessage` and log-only `internalDetails`; when to use `ConversionException.of()` vs throwing inline with `new ConversionException(...)`.
