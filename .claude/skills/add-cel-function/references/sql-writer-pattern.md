# The `SqlWriter` Callback Pattern

## Contents

- What `SqlWriter` is
- Why callbacks instead of pre-rendered strings
- How to use it in a dialect method
- Common mistakes

---

## What `SqlWriter` is

`SqlWriter` is a single-method functional interface in `dialect/SqlWriter.java`:

```java
@FunctionalInterface
public interface SqlWriter {
    void write() throws ConversionException;
}
```

The `Converter` passes `SqlWriter` lambdas to dialect methods. Each lambda, when invoked, appends sub-expression SQL to the shared `StringBuilder` that the converter is building.

## Why callbacks instead of pre-rendered strings

Three reasons:

1. **Dialects control output ordering.** Some dialects need to emit a function call wrapper around the sub-expression (`COALESCE(ARRAY_LENGTH(arr), 0)`); others embed it differently (`array_join(arr, ', ')`). If the converter pre-rendered the sub-expression, the dialect would have to re-parse or string-concat — error-prone for nesting and operator precedence.

2. **The shared `StringBuilder` keeps the output linear.** No intermediate strings are allocated; the dialect just calls `writeExpr.write()` at the right point and the right text appears in the right place.

3. **Errors propagate cleanly.** `SqlWriter.write()` throws `ConversionException`, so a failing sub-expression bubbles out without losing context.

## How to use it

A typical dialect method looks like this (from `PostgresDialect.writeJoin`):

```java
@Override
public void writeJoin(StringBuilder w, SqlWriter writeArray, SqlWriter writeDelim)
        throws ConversionException {
    w.append("ARRAY_TO_STRING(");
    writeArray.write();              // appends the array sub-expression here
    w.append(", ");
    if (writeDelim != null) {
        writeDelim.write();          // appends the delimiter sub-expression here
    } else {
        w.append("''");              // default empty delimiter
    }
    w.append(", '')");
}
```

The converter's call site (in `Converter.callJoin`) is:

```java
dialect.writeJoin(str, () -> visit(target), () -> visit(delim));
```

`() -> visit(target)` is the `SqlWriter` lambda. When invoked inside the dialect, it walks back into the converter to render the target expression in place.

## Common mistakes

- **Don't pre-render.** Never write `String s = visit(target); dialect.writeXxx(s);` — that's the anti-pattern this convention exists to prevent. Use the callback.
- **Don't capture state across calls.** A `SqlWriter` lambda may be invoked exactly once. If your dialect method needs the same sub-expression twice (e.g. for `(x IS NULL OR x = 0)`), the converter side must build it explicitly — open an issue rather than working around it.
- **Don't swallow exceptions.** `writeExpr.write()` throws `ConversionException`; the dialect method's `throws ConversionException` propagates it. Don't wrap in try/catch unless you have a clear recovery story.
- **Pass `null` for optional args.** Some methods (e.g. `writeJoin`) accept a nullable `SqlWriter` for optional positional arguments. Check `null` and emit a default literal in that case.
