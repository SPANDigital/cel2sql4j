# cel2sql4j

A Java library that converts [CEL (Common Expression Language)](https://cel.dev/) expressions into SQL WHERE clauses. Write filter expressions once in CEL and target any supported SQL dialect.

[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/com.spandigital/cel2sql4j)](https://central.sonatype.com/artifact/com.spandigital/cel2sql4j)

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MySQL](https://img.shields.io/badge/MySQL-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

## Features

- **6 SQL dialects** &mdash; PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Apache Spark
- **Parameterized queries** &mdash; safe placeholder-based output to prevent SQL injection
- **Index analysis** &mdash; dialect-specific index recommendations for your query patterns
- **JSON/JSONB support** &mdash; field access, existence checks, array operations
- **Array comprehensions** &mdash; `all`, `exists`, `exists_one`, `map`, `filter`
- **Timestamp arithmetic** &mdash; durations, intervals, EXTRACT, timezone handling
- **Regex with ReDoS protection** &mdash; RE2 pattern conversion per dialect
- **Security limits** &mdash; configurable max recursion depth and output length

## Quick Start

### Installation

**Gradle (Kotlin DSL)**

```kotlin
dependencies {
    implementation("com.spandigital:cel2sql4j:1.0.0-SNAPSHOT")
}
```

**Gradle (Groovy)**

```groovy
dependencies {
    implementation 'com.spandigital:cel2sql4j:1.0.0-SNAPSHOT'
}
```

**Maven**

```xml
<dependency>
    <groupId>com.spandigital</groupId>
    <artifactId>cel2sql4j</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Basic Conversion

```java
import com.spandigital.cel2sql.Cel2Sql;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.common.types.SimpleType;
import dev.cel.common.CelAbstractSyntaxTree;

// 1. Set up a CEL compiler with your variable declarations
CelCompiler compiler = CelCompilerFactory.standardCelCompilerBuilder()
    .addVar("name", SimpleType.STRING)
    .addVar("age", SimpleType.INT)
    .build();

// 2. Parse and check the CEL expression
CelAbstractSyntaxTree ast = compiler.compile("name == \"alice\" && age > 21")
    .getAst();

// 3. Convert to SQL (defaults to PostgreSQL)
String sql = Cel2Sql.convert(ast);
// => "name = 'alice' AND age > 21"
```

### Choosing a Dialect

```java
import com.spandigital.cel2sql.dialect.mysql.MySqlDialect;
import com.spandigital.cel2sql.dialect.sqlite.SqliteDialect;
import com.spandigital.cel2sql.dialect.duckdb.DuckDbDialect;
import com.spandigital.cel2sql.dialect.bigquery.BigQueryDialect;
import com.spandigital.cel2sql.dialect.postgres.PostgresDialect;
import com.spandigital.cel2sql.dialect.spark.SparkDialect;

// PostgreSQL (default)
String sql = Cel2Sql.convert(ast);

// MySQL
String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new MySqlDialect()));

// SQLite
String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new SqliteDialect()));

// DuckDB
String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new DuckDbDialect()));

// BigQuery
String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new BigQueryDialect()));

// Apache Spark
String sql = Cel2Sql.convert(ast, opts -> opts.withDialect(new SparkDialect()));
```

### Parameterized Queries

Literal values are replaced with placeholders and returned separately, keeping your queries safe from injection:

```java
import com.spandigital.cel2sql.ConvertResult;

ConvertResult result = Cel2Sql.convertParameterized(ast);

String sql = result.sql();
// PostgreSQL: "name = $1 AND age > $2"
// MySQL:      "name = ? AND age > ?"

List<Object> params = result.parameters();
// ["alice", 21]
```

### Index Analysis

Get dialect-specific index recommendations for your query patterns:

```java
import com.spandigital.cel2sql.AnalyzeResult;
import com.spandigital.cel2sql.dialect.IndexRecommendation;

AnalyzeResult result = Cel2Sql.analyzeQuery(ast,
    opts -> opts.withDialect(new PostgresDialect()));

System.out.println(result.sql());
// "name = 'alice' AND age > 21"

for (IndexRecommendation rec : result.recommendations()) {
    System.out.println(rec.column());     // "name", "age"
    System.out.println(rec.indexType());   // "BTREE"
    System.out.println(rec.expression());  // "CREATE INDEX idx_name ON table_name (name);"
    System.out.println(rec.reason());      // "Comparison operations on 'name' benefit from..."
}
```

### JSON/JSONB Field Schemas

Define schemas to enable JSON field access and type-aware conversions:

```java
import com.spandigital.cel2sql.schema.Schema;
import com.spandigital.cel2sql.schema.FieldSchema;

Schema schema = new Schema(Map.of(
    "metadata", new FieldSchema("metadata", "jsonb", false, 0, null, true, true, null)
));

String sql = Cel2Sql.convert(ast, opts -> opts
    .withDialect(new PostgresDialect())
    .withSchemas(Map.of("default", schema)));
```

### Configuration Options

```java
String sql = Cel2Sql.convert(ast, opts -> opts
    .withDialect(new PostgresDialect())               // SQL dialect (default: PostgreSQL)
    .withSchemas(schemas)                              // Schema map for JSON field detection
    .withJsonVariables("context", "tags")             // Flat JSONB columns (use ->> instead of .)
    .withColumnAliases(Map.of("name", "usr_name"))    // CEL identifier → SQL column rename
    .withParamStartIndex(5)                            // Embed in larger query: starts at $5
    .withMaxDepth(100)                                 // Max AST recursion depth (default: 100)
    .withMaxOutputLength(50000)                        // Max SQL output length (default: 50,000)
    .withLogger(myLogger));                            // SLF4J logger for debugging
```

**Flat JSONB columns** &mdash; mark variables typed as JSONB columns so dot-access uses
JSON operators rather than plain SQL dot notation:

```java
// CEL: context.host == "web-1"
// Without:  context.host = 'web-1'
// With withJsonVariables("context"):
//           context->>'host' = 'web-1'
```

**Column aliases** &mdash; map user-facing CEL names to actual database column names
(e.g. when columns are prefixed):

```java
String sql = Cel2Sql.convert(ast, opts -> opts
    .withColumnAliases(Map.of("name", "usr_name", "active", "usr_active")));
// CEL: name == "Alice" → SQL: usr_name = 'Alice'
```

**Parameter start index** &mdash; useful when embedding the generated SQL into a
larger pre-parameterized query so placeholders don't clash:

```java
ConvertResult res = Cel2Sql.convertParameterized(ast, opts -> opts
    .withParamStartIndex(5));
// PostgreSQL: name = $5 AND age > $6
// Caller appends res.parameters() to their args at positions 5+.
```

### Supported CEL Functions

| Category | Functions |
|---|---|
| String predicates | `contains`, `startsWith`, `endsWith`, `matches`, `size` |
| String manipulation | `lowerAscii`, `upperAscii`, `trim`, `charAt`, `indexOf`, `lastIndexOf`, `substring`, `replace`, `reverse`, `split`, `join`, `format` |
| Type conversion | `bool`, `bytes`, `double`, `int`, `string`, `duration`, `timestamp` |
| Timestamp extraction | `getFullYear`, `getMonth`, `getDate`, `getDayOfMonth`, `getDayOfYear`, `getDayOfWeek`, `getHours`, `getMinutes`, `getSeconds`, `getMilliseconds` |
| Comprehensions | `all`, `exists`, `exists_one`, `map`, `filter` |

> **Note on `format()`** &mdash; supported on PostgreSQL (via `FORMAT()`), BigQuery (via `FORMAT()`),
> SQLite and DuckDB (via `printf()`). MySQL throws because it has no printf-style FORMAT.
> Only `%s`, `%d`, `%f` specifiers are accepted; format strings are bounded to 1000 characters.

### Resource Limits

cel2sql4j enforces several caps to keep generated SQL safe and bounded:

| Limit | Default | Configurable | Purpose |
|---|---|---|---|
| Recursion depth | 100 | `withMaxDepth(int)` | Prevent stack overflow on deeply nested CEL |
| SQL output length | 50 000 chars | `withMaxOutputLength(int)` | Cap total generated SQL size |
| Byte literal length (inline mode) | 10 000 bytes | constant | Prevent CWE-400 hex expansion (parameterized mode bypasses this — bytes go straight to JDBC) |
| `format()` string length | 1 000 chars | constant | Bound expansion in formatted SQL |

## Supported Dialects

| Dialect | Placeholders | Regex | Arrays | JSON | Index Types |
|---------|-------------|-------|--------|------|-------------|
| **PostgreSQL** | `$1, $2` | `~` / `~*` (POSIX) | `ANY()`, `UNNEST()` | `->`, `->>`, `jsonb_` | BTREE, GIN |
| **MySQL** | `?, ?` | `REGEXP` (ICU) | JSON functions | `->`, `->>`, `JSON_` | BTREE, FULLTEXT |
| **SQLite** | `?, ?` | Not supported | JSON functions | `->`, `->>`, `json_` | BTREE |
| **DuckDB** | `$1, $2` | `~` / `~*` (RE2) | `ANY()`, `UNNEST()` | `->`, `->>`, `json_` | ART |
| **BigQuery** | `@p1, @p2` | `REGEXP_CONTAINS` | `UNNEST()`, arrays | `JSON_VALUE`, `JSON_QUERY` | CLUSTERING, SEARCH_INDEX |
| **Apache Spark** | `?, ?` | `RLIKE` (Java regex) | `array()`, `array_contains` | `get_json_object`, `from_json` | storage-specific (none emitted) |

## CEL Expression Examples

| CEL Expression | PostgreSQL Output |
|---|---|
| `age > 21` | `age > 21` |
| `name == "alice"` | `name = 'alice'` |
| `active == true` | `active = TRUE` |
| `name == "a" && age > 18` | `name = 'a' AND age > 18` |
| `status == "active" \|\| role == "admin"` | `status = 'active' OR role = 'admin'` |
| `email.startsWith("admin")` | `email LIKE 'admin%' ESCAPE E'\\'` |
| `name.contains("test")` | `POSITION('test' IN name) > 0` |
| `name.matches("^a.*z$")` | `name ~ '^a.*z$'` |
| `"admin" in roles` | `'admin' = ANY(roles)` |
| `scores.all(s, s >= 60)` | `NOT EXISTS (SELECT 1 FROM UNNEST(scores) AS s WHERE NOT (s >= 60))` |
| `age > 18 ? "adult" : "minor"` | `CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END` |

## Building from Source

```bash
# Clone the repository
git clone https://github.com/spandigital/cel2sql4j.git
cd cel2sql4j

# Build
./gradlew build

# Run tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.spandigital.cel2sql.Cel2SqlBasicTest"
```

**Requirements:** Java 17 or later.

## Origin

This library is a Java port of [cel2sql](https://github.com/spandigital/cel2sql), originally written in Go. It preserves the same API design, dialect coverage, and test cases.

## License

[MIT](LICENSE) &copy; Span Digital
