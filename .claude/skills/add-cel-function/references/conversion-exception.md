# `ConversionException`: User-Safe vs Internal Details

## Contents

- The two-message split
- Factory methods
- When to use which form
- What goes in each field

---

## The two-message split

`ConversionException` (in `error/ConversionException.java`) deliberately separates two strings:

```java
public class ConversionException extends Exception {
    private final String userMessage;       // safe to surface to end users
    private final String internalDetails;   // for logs only — may include internals
}
```

This is the **CWE-209** pattern (Information Exposure Through an Error Message): a user submitting a malformed CEL expression should see something like *"Invalid function arguments"*, while the operator's logs see *"format() argument count mismatch: format has 2 placeholders but got 3 arguments"*.

`getMessage()` returns `userMessage`. `getInternalDetails()` returns the details (or `userMessage` if none was set).

## Factory methods

Use these instead of calling the constructor directly:

```java
ConversionException.of(userMessage, internalDetails)
ConversionException.of(userMessage, internalDetails, cause)
ConversionException.wrap(cause, internalContext)        // userMessage = cause.getMessage()
```

Inline-throwing the constructor is also fine when you want to keep the file readable:

```java
throw new ConversionException(
    ErrorMessages.INVALID_ARGUMENTS,
    "format() argument count mismatch: format has " + n + " placeholders but got " + m);
```

`ErrorMessages` (in the `error` package) is a small set of canonical user-facing strings (`UNSUPPORTED_EXPRESSION`, `INVALID_ARGUMENTS`, `CONVERSION_FAILED`, etc.). Reuse them.

## When to use which form

| Situation | Use |
|---|---|
| New unsupported feature in a single dialect | `ConversionException.of("Unsupported operation", "<feature> is not supported in <Dialect>: <reason>")` — see `MySqlDialect.writeFormat`, `SqliteDialect.writeRegexMatch`, `SparkDialect.writeJSONArrayMembership` |
| Bad arity / wrong shape from the AST | `new ConversionException(ErrorMessages.INVALID_ARGUMENTS, "<fn>() requires X args, got " + n)` |
| Bad type from the type-checker | `new ConversionException(ErrorMessages.UNSUPPORTED_EXPRESSION, "<fn>() requires <type>, got " + actual)` |
| Wrapping a cause from a sub-call (rare) | `ConversionException.wrap(cause, "while building <context>")` |

## What goes in each field

**`userMessage` — user-safe, vague enough.** Limit to the fixed strings in `ErrorMessages`, plus short categorical labels like `"Unsupported operation"`, `"Invalid pattern in expression"`, `"Invalid field name"`. **Never include user input or internal identifiers** — those leak.

**`internalDetails` — concrete, verbose, useful for triage.** Include the function name, the offending value or count, the dialect, the file/line if you have it. Example:

```
"byte literal length 10001 exceeds maximum of 10000"
"format() unsupported specifier '%x': only %s, %d, %f are allowed"
"Spark dialect does not support multi-dimensional array length (dimension=2)"
```

When asserting on internal details in tests, use `getInternalDetails()` directly:

```java
ConversionException ex = catchConversionException(() -> Cel2Sql.convert(ast, opts));
assertThat(ex.getInternalDetails()).contains("byte literal length").contains("10001");
```

`hasMessageContaining(...)` only checks `userMessage` — by design.
