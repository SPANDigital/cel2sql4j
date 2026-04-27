# Test Files: Where to Add the New Dialect

When adding a new dialect, every test class with dialect-looping `Arguments.of(name, celExpr, "<DialectLabel>", DIALECT, expectedSql)` cases needs the new dialect added. Use this list as the checklist.

## Inventory

All under `src/test/java/com/spandigital/cel2sql/`:

| File | Cases | Notes |
|---|---|---|
| `Cel2SqlBasicTest.java` | comparisons, boolean/null, AND/OR | Smallest; quickest to update first. |
| `Cel2SqlOperatorTest.java` | comparison + arithmetic operators | |
| `Cel2SqlCastTest.java` | `int(x)`, `string(x)`, `double(x)`, etc. | Largest cast-style cases. |
| `Cel2SqlStringTest.java` | `contains`, `startsWith`, `endsWith`, `matches`, `size` | |
| `Cel2SqlArrayTest.java` | `[1,2,3][0]`, `size(arr)`, `in [...]` | Watch for `COALESCE(ARRAY_LENGTH(...), 0)` shape. |
| `Cel2SqlComprehensionTest.java` | `all`, `exists`, `exists_one`, `map`, `filter` | Most complex SQL — careful with subquery scaffolding. |
| `Cel2SqlTimestampTest.java` | `getFullYear`, `getMonth`, `getDayOfWeek`, etc. | `getDayOfWeek` is the per-dialect adjustment trap (see dialect-method-checklist). |
| `Cel2SqlRegexTest.java` | `matches(pattern)` | If your dialect doesn't support regex, add throw-tests rather than parameterized cases. |
| `Cel2SqlParameterizedTest.java` | parameterized output (placeholders) | Just verify the placeholder shape is correct (`$1` vs `?` vs `@p1`). |
| `Cel2SqlAnalyzeTest.java` | `analyzeQuery` recommendations | Skip if your dialect doesn't implement `IndexAdvisor`. |
| `Cel2SqlOptionsTest.java` | `withColumnAliases`, `withParamStartIndex`, `withJsonVariables`, byte cap, format | Add a Spark-style `parameterizedUsesPositionalQuestionMark`-equivalent if your placeholder shape differs from `$1`. |

## Existing dialect-specific test classes

These are full focused-test classes for one dialect — add a parallel class only if your new dialect has unique behaviour worth testing in isolation (Spark needed it for the JSON-array-membership throw):

- `Cel2SqlSparkTest.java` — Spark-specific behaviour (`RLIKE`, `EXPLODE`, `dayofweek - 1`, `array_contains`, IndexAdvisor returning empty list).

## Conventions

- **`@MethodSource("xxxTests")`** with `Stream<Arguments>` is the only test pattern. Don't introduce new test framework conventions — see `CLAUDE.md` "Test Patterns" section.
- **`CelHelper.standardCompiler()`** in `testutil/CelHelper.java` declares the standard variables (`name:string`, `age:int`, `created_at:timestamp`, `tags:list<string>`, etc.). If your test needs a new var, add it to `CelHelper` rather than constructing a custom compiler — unless you need a one-off (like `Cel2SqlOptionsTest.jsonVarCompiler` or `stringsExtCompiler`).
- **Dialect constants at the top:** `private static final Dialect SPARK = new SparkDialect();` (mirror existing files).
- **Group cases by feature** with a comment line, mirroring the existing structure: `// size_list: array size only for PG/DuckDB/BQ`.

## Smoke check after editing

```bash
./gradlew test                                 # full unit suite
./gradlew test --tests "*OptionsTest"          # focused — fast
```

Integration tests (`./gradlew integrationTest`) require Docker/Testcontainers and only cover PG + MySQL today. New dialects don't need integration tests in v1 unless you spin up a Testcontainer for them.
