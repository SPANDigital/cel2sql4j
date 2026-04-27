# Go to Java Idiom Mapping

## Contents

- Closures and callbacks
- Error handling
- Sentinel errors
- Type providers and reflection
- Generics
- Slices, maps, ordered maps
- Strings.Builder vs StringBuilder
- Tests
- What does NOT port

---

## Closures and callbacks

Go upstream uses `func() error` closures throughout the dialect interface:

```go
func (d *Dialect) WriteContains(w *strings.Builder, writeHaystack, writeNeedle func() error) error {
    if err := writeHaystack(); err != nil {
        return err
    }
    ...
}
```

cel2sql4j uses the `SqlWriter` functional interface (`dialect/SqlWriter.java`):

```java
@FunctionalInterface
public interface SqlWriter {
    void write() throws ConversionException;
}
```

Java equivalent of the above:

```java
public void writeContains(StringBuilder w, SqlWriter writeHaystack, SqlWriter writeNeedle)
        throws ConversionException {
    writeHaystack.write();
    ...
}
```

The exception propagates instead of an explicit return.

## Error handling

Go's wrapped sentinel-error pattern:

```go
return fmt.Errorf("%w: format() requires a constant format string", ErrUnsupportedOperation)
```

Java equivalent — single `ConversionException` with two-string split (CWE-209):

```java
throw new ConversionException(
    ErrorMessages.UNSUPPORTED_EXPRESSION,
    "format() requires a constant format string");
```

`ErrorMessages` (in the `error` package) holds canonical user-safe strings (`UNSUPPORTED_EXPRESSION`, `INVALID_ARGUMENTS`, `CONVERSION_FAILED`, `INVALID_TIMESTAMP_OP`, etc.). The second arg is for logs; reuse the precedent in the existing dialects.

For the unsupported-feature pattern:

```java
throw ConversionException.of(
    "Unsupported operation",
    "<feature> is not supported in <Dialect>: <reason>");
```

## Sentinel errors

Go has 16 `Err*` sentinels (`ErrUnsupportedExpression`, `ErrInvalidFieldName`, etc.) for `errors.Is`. Java does not — there's a single `ConversionException`. **Don't port the sentinels.** If you find yourself wanting `if errors.Is(err, ErrFoo)`, translate to:

- A check on `getUserMessage()` against an `ErrorMessages` constant, or
- A subclass of `ConversionException` (rare; only if callers really need to branch on it).

For tests, use `getInternalDetails()` to assert on the verbose message — `getMessage()` is intentionally vague.

## Type providers and reflection

Go upstream ships `pg/provider.go`, `mysql/provider.go`, etc. — JDBC-introspection providers that build `Schema` objects from a live connection. **These don't port to Java.** Java users construct `Schema` directly from application metadata. Add a note to CLAUDE.md's "Differences from upstream" section if upstream changes a provider.

Same applies to YAML/JSON loaders for `FieldSchema` that use Go struct tags + reflection — the Java port doesn't have a YAML loader; users build the records directly.

## Generics

Go's generics are recent and rare in cel2sql; most "generic-looking" code is empty-interface (`interface{}`) which maps cleanly to Java's `Object` (or `CelExpr` / `CelType` from cel-java). Where Go uses real generics (`func Map[T, U](...)`), Java's parametric polymorphism applies — usually 1:1.

## Slices, maps, ordered maps

| Go | Java |
|---|---|
| `[]string` | `List<String>` (immutable preferred — `List.copyOf`, `ImmutableList`) |
| `map[string]bool` | `Set<String>` (yes, Set — Go uses `map[k]bool` as a set idiom) |
| `map[string]string` | `Map<String, String>` (use `LinkedHashMap` if order matters) |
| `map[string]Schema` | `Map<String, Schema>` |

When porting iteration, beware: Java's `Map.entrySet()` order is not guaranteed unless you use `LinkedHashMap`. Go's `map` iteration order is randomised, so upstream code that relies on iteration order would already be buggy — don't replicate that.

## Strings.Builder vs StringBuilder

Direct mapping:

| Go | Java |
|---|---|
| `strings.Builder` | `StringBuilder` |
| `w.WriteString("foo")` | `w.append("foo")` |
| `fmt.Fprintf(w, "INTERVAL %d %s", n, unit)` | `w.append("INTERVAL ").append(n).append(' ').append(unit)` |
| `w.WriteString(strconv.FormatInt(n, 10))` | `w.append(n)` (StringBuilder has long overload) |

cel2sql4j passes a single shared `StringBuilder` through the call chain — same as Go's pattern of passing `*strings.Builder`.

## Tests

Go's table-driven tests:

```go
testCases := []struct{ name, cel, sql string }{
    {"contains", `name.contains("x")`, "POSITION('x' IN name) > 0"},
    ...
}
for _, tc := range testCases {
    t.Run(tc.name, func(t *testing.T) { ... })
}
```

Java's JUnit 5 parameterized equivalent:

```java
static Stream<Arguments> contains() {
    return Stream.of(
        Arguments.of("contains", "name.contains(\"x\")", PG, "POSITION('x' IN name) > 0"),
        ...
    );
}

@ParameterizedTest(name = "{0}")
@MethodSource("contains")
void testContains(String name, String cel, Dialect dialect, String expected) throws Exception {
    var ast = CelHelper.compile(cel);
    assertThat(Cel2Sql.convert(ast, opts -> opts.withDialect(dialect))).isEqualTo(expected);
}
```

For dialect-looping tests, add cases for every dialect explicitly — cel2sql4j doesn't have a "run for all dialects" macro.

## CEL Java vs cel-go

The Java CEL library (`dev.cel`) differs from `cel-go` in small but important ways. Three to remember:

- `CelExpr.getKind()` returns a `Kind` enum, not a protobuf oneof.
- `ast.getType(expr.id())` returns `Optional<CelType>`.
- The strings extension (`CelExtensions.strings()`) is **smaller** than `cel-go`'s — it includes `split`/`join`/`charAt`/`indexOf`/`lastIndexOf`/`replace`/`substring`/`trim`/`lowerAscii`/`upperAscii`, but **not `format()`**. Tests that use `format()` must declare it manually as a `CelFunctionDecl` (see `Cel2SqlOptionsTest.stringsExtCompiler`).

## What does NOT port

| Upstream Go thing | Why it stays in Go |
|---|---|
| `pg/provider.go`, `mysql/provider.go`, etc. (JDBC type loaders) | Java users construct `Schema` directly. |
| 16 `Err*` sentinel errors | Java uses single `ConversionException` (CWE-209). |
| Name-based numeric-cast heuristic (`score`/`value`/`amount`/etc. → `::numeric`) | Java never had it; explicit casts required. |
| YAML/JSON deserialization via struct tags | Not idiomatic; Java users build records directly. |
| Comprehension pattern-matching tightening | Java's comprehension match is structurally different. |
