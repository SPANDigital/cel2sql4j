# Dialect Interface Method Checklist

## Contents

- Literals (3)
- Operators (4)
- Type Casting (4)
- Arrays (5)
- JSON (7)
- Timestamps (4)
- String Functions (4)
- Comprehensions (3, +1 default)
- Struct (2)
- Validation (3)
- Regex (2)
- Capabilities (4)

Total: ~41 methods. Use the existing dialects as living references — `DuckDbDialect.java` is the most "modern, complete" template.

---

## Literals (3)

| Method | What to emit |
|---|---|
| `name()` | Return the `DialectName` enum constant. |
| `writeStringLiteral(w, value)` | Quoted string with single-quote doubling (or `\\'` for BigQuery). |
| `writeBytesLiteral(w, value)` | `X'HEX'` (most), `'\\xHEX'` (Postgres/DuckDB), `b"\\OCT..."` (BigQuery). |
| `writeParamPlaceholder(w, idx)` | `$N` (PG/DuckDB), `?` (MySQL/SQLite/Spark), `@pN` (BigQuery). |

## Operators (4)

| Method | What to emit |
|---|---|
| `writeStringConcat(w, lhs, rhs)` | `lhs \|\| rhs`, `CONCAT(lhs, rhs)`, or `concat(lhs, rhs)` per dialect. |
| `writeRegexMatch(w, target, pattern, caseInsensitive)` | `target ~ '...'`, `target REGEXP '...'`, `target RLIKE '...'`, `REGEXP_CONTAINS(target, '...')`. |
| `writeLikeEscape(w)` | The trailing `ESCAPE '...'` clause for `LIKE`. BigQuery is empty. |
| `writeArrayMembership(w, elem, arr)` | `elem = ANY(arr)`, `array_contains(arr, elem)`, `elem IN UNNEST(arr)`, etc. |

## Type Casting (4)

| Method | What to emit |
|---|---|
| `writeCastToNumeric(w)` | Postfix: `::numeric` (PG), `::DOUBLE` (DuckDB), ` + 0` (MySQL/SQLite/Spark), `::FLOAT64` (BigQuery). |
| `writeTypeName(w, celTypeName)` | Map `bool`/`bytes`/`double`/`int`/`string`/`uint` to the dialect-native name. |
| `writeEpochExtract(w, expr)` | `EXTRACT(EPOCH FROM expr)`, `UNIX_TIMESTAMP(expr)`, `UNIX_SECONDS(expr)`, `CAST(strftime('%s', expr) AS INTEGER)`. |
| `writeTimestampCast(w, expr)` | `CAST(expr AS TIMESTAMP)` / `TIMESTAMPTZ` / `DATETIME` / `datetime(expr)`. |

## Arrays (5)

| Method | What to emit |
|---|---|
| `writeArrayLiteralOpen(w)` / `writeArrayLiteralClose(w)` | Opening + closing brackets for `[1, 2, 3]`. |
| `writeArrayLength(w, dimension, expr)` | **Wrap in `COALESCE(..., 0)`** so `size(NULL)` is 0 and `size(arr) > 0` works correctly. Throw on `dimension > 1` if the dialect can't do multi-dim. |
| `writeListIndex(w, arr, idx)` / `writeListIndexConst(w, arr, idx)` | Convert CEL's 0-indexing to dialect's 1-indexing where applicable (PG/DuckDB use `+ 1` or literal `idx + 1`; BigQuery uses `[OFFSET(idx)]`). |
| `writeEmptyTypedArray(w, typeName)` | `ARRAY[]::TYPE[]`, `[]::TYPE[]`, `ARRAY<TYPE>[]`, `JSON_ARRAY()`, `CAST(array() AS ARRAY<TYPE>)`. |

## JSON (7)

| Method | What to emit |
|---|---|
| `writeJSONFieldAccess(w, base, field, isFinal)` | `base->>'field'` (final/text) vs `base->'field'` (intermediate/JSON). BigQuery: `JSON_VALUE` vs `JSON_QUERY`. |
| `writeJSONExistence(w, isJSONB, field, base)` | `base ? 'field'` (PG JSONB), `base->'field' IS NOT NULL` (PG JSON), `JSON_CONTAINS_PATH(...)`, `json_type(...) IS NOT NULL`, `get_json_object(...) IS NOT NULL`. |
| `writeJSONArrayElements(w, isJSONB, asText, expr)` | Set-returning function: `jsonb_array_elements_text`, `json_each`, `JSON_TABLE`, `UNNEST(JSON_QUERY_ARRAY(...))`, `EXPLODE(from_json(..., 'ARRAY<STRING>'))`. |
| `writeJSONArrayLength(w, expr)` | **Wrap in `COALESCE(..., 0)`.** `jsonb_array_length`, `JSON_LENGTH`, `json_array_length`, `ARRAY_LENGTH(JSON_QUERY_ARRAY(...))`, `size(from_json(...))`. |
| `writeJSONExtractPath(w, segments, root)` | Path-style existence check used by `has(json.a.b.c)`. Postgres: `jsonb_extract_path_text(...) IS NOT NULL`. |
| `writeJSONArrayMembership(w, jsonFunc, expr)` | **Caveat:** the converter wraps this with `lhs = `, which only works on dialects whose JSON-array-elements function returns a relation. SQLite is silently buggy here; Spark throws. Match the convention of the closest existing dialect. |
| `writeNestedJSONArrayMembership(w, expr)` | Same as above for nested chains. |

## Timestamps (4)

| Method | What to emit |
|---|---|
| `writeDuration(w, value, unit)` | `INTERVAL N <unit>`. SQLite uses `'+N <unit>s'` strftime modifier syntax. |
| `writeInterval(w, value, unit)` | Same shape but with a sub-expression value. |
| `writeExtract(w, part, expr, tz)` | `EXTRACT(<part> FROM expr [AT TIME ZONE tz])`. **`getDayOfWeek` needs adjustment**: Postgres/DuckDB emit `(EXTRACT(DOW FROM expr) + 6) % 7`; MySQL `(DAYOFWEEK(expr) + 5) % 7`; Spark `(dayofweek(expr) - 1)`; SQLite `CAST(strftime('%w', expr) AS INTEGER)`. CEL convention is 0=Monday..6=Sunday. |
| `writeTimestampArithmetic(w, op, ts, dur)` | `ts + dur` or `ts - dur`. |

## String Functions (4)

| Method | What to emit |
|---|---|
| `writeContains(w, haystack, needle)` | `POSITION(needle IN haystack) > 0` (PG), `LOCATE(needle, haystack) > 0` (MySQL/Spark), `INSTR(haystack, needle) > 0` (SQLite), `CONTAINS(haystack, needle)` (DuckDB), `STRPOS(haystack, needle) > 0` (BigQuery). |
| `writeSplit(w, str, delim)` | `STRING_TO_ARRAY` (PG), `split` (Spark), `STRING_SPLIT` (DuckDB). MySQL/SQLite throw — no native split. |
| `writeSplitWithLimit(w, str, delim, limit)` | Same with limit. Many dialects emit `(<expr>)[1:limit]` slice. |
| `writeJoin(w, arr, delim)` | `ARRAY_TO_STRING` (most), `array_join` (Spark), `JSON_UNQUOTE` (MySQL — limited). |

## Comprehensions (3 + 1 default)

| Method | What to emit |
|---|---|
| `writeUnnest(w, src)` | `UNNEST(src)`, `EXPLODE(src)`, `json_each(src)`, `JSON_TABLE(...)`. |
| `writeComprehensionSource(w, src, iterVar)` | Default: `<unnest> AS iterVar`. Override only if the dialect needs different aliasing (DuckDB uses `AS _t(iterVar)`). |
| `writeArraySubqueryOpen(w)` / `writeArraySubqueryExprClose(w)` | The wrapping that turns the expression into an array-building subquery. PG: `ARRAY(SELECT ...)`. Spark: `(SELECT collect_list(...))`. |

## Struct (2)

| Method | What to emit |
|---|---|
| `writeStructOpen(w)` / `writeStructClose(w)` | `ROW(...)`, `struct(...)`, etc. |

## Validation (3)

| Method | What to emit |
|---|---|
| `maxIdentifierLength()` | `0` for "no limit"; positive int for the dialect's max (Spark: 128). |
| `validateFieldName(name)` | Delegate to `<Name>Validation.validateFieldName(name)`. |
| `reservedKeywords()` | Delegate to `<Name>Validation.getReservedKeywords()`. |

## Regex (2)

| Method | What to emit |
|---|---|
| `convertRegex(re2Pattern)` | Delegate to `<Name>Regex.convertRE2To<Name>(re2Pattern)`. Returns `RegexResult(pattern, caseInsensitive)`. **For Java-regex-engine dialects (Spark)**, the pattern passes through unchanged after security checks — `(?i)` is honoured natively, so `caseInsensitive` is reported as `false`. |
| `supportsRegex()` | `true`/`false`. SQLite returns `false` and throws from `writeRegexMatch`. |

## Capabilities (4)

| Method | What to emit |
|---|---|
| `supportsRegex()` | (See above.) |
| `supportsNativeArrays()` | `true` (PG/DuckDB/BigQuery/Spark), `false` (MySQL/SQLite — both store arrays as JSON). |
| `supportsJSONB()` | `true` (PG only), `false` (everyone else). |
| `supportsIndexAnalysis()` | `true` for the five real-database dialects, `false` for Spark (storage-layer-specific). |
