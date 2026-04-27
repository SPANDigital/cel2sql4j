---
name: add-sql-dialect
description: Adds a new SQL dialect to cel2sql4j (CockroachDB, Snowflake, Redshift, ClickHouse, etc.) by scaffolding the dialect package with `<Name>Dialect` / `<Name>Validation` / `<Name>Regex` classes, registering the dialect in the `DialectName` enum, deciding whether to implement `IndexAdvisor`, threading new test cases through every dialect-looping test class, and updating the README badge grid and dialect-comparison tables. Use when porting a new database target.
---

# Add a SQL Dialect

cel2sql4j currently supports six dialects: PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Apache Spark. Adding a seventh follows the pattern that the Spark dialect (PR #10) and the original five established. This skill captures that pattern.

## Quick start

```bash
# Scaffold the package by copying the closest existing dialect.
.claude/skills/add-sql-dialect/scripts/scaffold_dialect.sh duckdb cockroach Cockroach
#                                                          ^^^^^^^ template
#                                                                  ^^^^^^^^^ folder/identifier
#                                                                            ^^^^^^^^^ class prefix

# Then:
#   1. Fill in SQL bodies in dialect/cockroach/CockroachDialect.java
#   2. Update DialectName enum
#   3. Add SPARK-style cases to all dialect-looping tests
#   4. Update README + CLAUDE.md dialect-comparison tables
#   5. ./gradlew build
```

## Choosing a template

The scaffold script copies an existing dialect package and renames classes. Pick the closest match:

| If your target dialect has... | Template |
|---|---|
| Native arrays, RE2-compatible regex (`~` / `~*`), `JSON` column type | `duckdb` |
| Native arrays, POSIX regex, `JSONB` column type | `postgres` |
| Native arrays, Java regex (`RLIKE`), `STRING`-typed JSON | `spark` |
| `?` placeholders, JSON-as-text storage, `REGEXP` operator | `mysql` |
| `?` placeholders, no native arrays, `json_extract`-style JSON | `sqlite` |
| `@pN` placeholders, `JSON_VALUE` / `JSON_QUERY` | `bigquery` |

DuckDB is the most "modern-PostgreSQL-like" template and is usually the right pick for new analytical engines (CockroachDB, Snowflake, ClickHouse).

## File touchpoints

The scaffold creates `<name>Dialect.java`, `<name>Validation.java`, `<name>Regex.java` in `src/main/java/com/spandigital/cel2sql/dialect/<name>/`. After scaffolding, update these manually:

### Code

- **`dialect/<name>/<Name>Dialect.java`** — implement all ~35 `Dialect` methods. See [references/dialect-method-checklist.md](references/dialect-method-checklist.md) for the full list and emission notes.
- **`dialect/<name>/<Name>Validation.java`** — supply the dialect's reserved-keyword set and identifier-shape regex. Lowercase the keyword list.
- **`dialect/<name>/<Name>Regex.java`** — only if the dialect supports regex. The `SparkRegex` / `DuckDbRegex` files are the cleanest templates for "RE2-superset" engines (most modern engines).
- **`dialect/DialectName.java`** — add the enum constant.

### Tests

Add `Arguments.of(name, celExpr, "<DialectLabel>", DIALECT_INSTANCE, expectedSql)` cases to every dialect-looping test. See [references/test-files.md](references/test-files.md) for the complete inventory of test files and where dialect cases live.

### Docs

- **`README.md`** — badge in the badge grid; row in the "Supported Dialects" comparison table; import line in the "Choosing a Dialect" code example. Bump the `**6 SQL dialects**` count.
- **`CLAUDE.md`** — column in the dialect-comparison table under "Dialect System".

## The IndexAdvisor decision

A dialect can optionally implement `IndexAdvisor` (see `dialect/IndexAdvisor.java`). Two paths:

- **Implement with real recommendations.** PostgreSQL, MySQL, SQLite, DuckDB, BigQuery follow this pattern. Look at `DuckDbDialect.recommendIndex` for the cleanest example.
- **Implement returning null** (mirrors `SparkDialect`). Use this if the dialect's index types are storage-layer-specific (Spark: Delta vs Iceberg vs Parquet; some columnar warehouses similar). Implementing the interface (rather than omitting it) prevents `Cel2Sql.analyzeQuery` from silently falling back to Postgres recommendations:

```java
@Override
public IndexRecommendation recommendIndex(IndexPattern pattern) { return null; }

@Override
public List<PatternType> supportedPatterns() { return List.of(); }
```

## Capabilities methods

The four `supportsXxx()` methods on `Dialect` are not just informational — the `Converter` reads `supportsRegex()` to decide whether to throw on `expr.matches(...)`, `supportsJSONB()` to pick `->` vs `JSON_VALUE` semantics, and so on. Set them honestly:

```java
@Override public boolean supportsRegex()         { return true; }
@Override public boolean supportsNativeArrays()  { return true; }
@Override public boolean supportsJSONB()         { return false; }
@Override public boolean supportsIndexAnalysis() { return false; }
```

If a dialect doesn't support regex, return `false` from `supportsRegex()` AND throw a clear `ConversionException` from `writeRegexMatch` (mirror `SqliteDialect`). Don't return `true` and emit broken SQL.

## Verification

```bash
# Lint the new skill if you also touched it
python3 .claude/skills/skill-authoring/scripts/lint_skill.py .claude/skills/add-sql-dialect

# Build + test (must be green before opening a PR)
./gradlew build
```

Smoke-test by running one expression through the new dialect:

```java
String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new CockroachDialect()));
```

## Scripts

- **Run** `.claude/skills/add-sql-dialect/scripts/scaffold_dialect.sh <template> <new-name> <NewClassPrefix>` — copies `dialect/<template>/` to `dialect/<new-name>/` and renames classes / strings via `sed`. Output is the list of files created. The agent then fills in the SQL bodies.

## References

- [references/dialect-method-checklist.md](references/dialect-method-checklist.md) — every method on the `Dialect` interface grouped by category, with one-line "what to emit" guidance per method.
- [references/test-files.md](references/test-files.md) — every test class that has dialect-looping `Arguments.of(...)` cases; the new dialect must be added to each.
