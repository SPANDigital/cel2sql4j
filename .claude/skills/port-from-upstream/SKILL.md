---
name: port-from-upstream
description: Ports features and bug fixes from the upstream Go library github.com/spandigital/cel2sql (locally checked out at /Users/richardwooding/Code/SPAN/cel2sql) into the Java cel2sql4j port. Use when syncing upstream changes — the workflow enumerates candidate commits since the last sync, verifies whether each change is already applied (many turn out to be done already), maps Go idioms to Java equivalents, and updates tests plus the "Differences from upstream" section in CLAUDE.md.
---

# Port from Upstream

cel2sql4j is a Java port of the Go library `github.com/spandigital/cel2sql`. Upstream ships features and fixes regularly; this skill captures the workflow for syncing them across.

## Quick start

```bash
# 1. Enumerate candidate upstream commits since the last sync.
.claude/skills/port-from-upstream/scripts/list_upstream_changes.sh

# 2. For each candidate, grep cel2sql4j first — many fixes are already done.
#    See the "Pre-port verification" section below.

# 3. For real porting work: read the upstream commit's diff, map the Go code
#    to Java idioms (see references/go-to-java-idioms.md), and write a test
#    case asserting the same expected SQL.

# 4. If two unrelated upstream commits are bundled (e.g. a new dialect plus
#    feature flags), split into two PRs — see references/two-pr-shape.md.
```

## Pre-port verification: always grep first

A surprising fraction of upstream "fixes" turn out to be already implemented in cel2sql4j — the original Java port pulled some patches from a fork or pre-empted them. **Don't write code before verifying.** Examples from the v3.7.1 backport:

| Upstream commit / fix | Already done? |
|---|---|
| `getDayOfWeek` modulo correction | Yes — `Cel2SqlTimestampTest:130` had the right SQL all along |
| `EXTRACT(... AT TIME ZONE ...)` syntax | Yes — all relevant dialects emit the correct form |
| `ARRAY_LENGTH` wrapped in `COALESCE` | 4 of 5 dialects already correct; only BigQuery needed the fix |
| Removal of name-based numeric cast heuristic | Java never had this heuristic |
| 16 sentinel-error refactor | N/A — Java uses single `ConversionException`, intentional |

Probes for the most common items:

```bash
# Is X already wrapped in COALESCE?
grep -nA4 "writeArrayLength\|writeJSONArrayLength" \
  src/main/java/com/spandigital/cel2sql/dialect/*/[A-Z]*Dialect.java

# Is "AT TIME ZONE" used (vs just "AT")?
grep -rn "\" AT \"\|AT TIME ZONE" src/main/java/com/spandigital/cel2sql/dialect/

# Does Java's day-of-week emit the modulo adjustment?
grep -nA3 "TIME_GET_DAY_OF_WEEK\|getDayOfWeek" \
  src/main/java/com/spandigital/cel2sql/Converter.java \
  src/test/java/com/spandigital/cel2sql/Cel2SqlTimestampTest.java
```

Note any "already done" finding in the PR description rather than silently skipping it.

## Out of scope (mirrors upstream rejections)

These upstream concerns intentionally do **not** port to cel2sql4j. If you encounter them, add a note to CLAUDE.md's "Differences from upstream" section rather than implementing them:

- **JDBC schema providers** (`pg/provider.go`, `mysql/provider.go`, etc.) — Java users construct `Schema` directly from application metadata; runtime introspection isn't a portable contract.
- **16 sentinel error types** (`ErrUnsupportedExpression`, `ErrInvalidFieldName`, etc.) — Java uses single `ConversionException` with `userMessage` / `internalDetails` (CWE-209). Idiomatic for the JVM.
- **Name-based numeric-cast heuristic** (auto-cast of `score` / `value` / `count` / etc. to numeric) — Java never had it; explicit `int(x)` / `double(x)` casts are required.
- **Comprehension pattern-matching tightening** — Java's comprehension matching is structurally different and doesn't have the same false-positive surface.

## Workflow per upstream commit

1. **Read** the upstream commit's diff: `git -C /Users/richardwooding/Code/SPAN/cel2sql show <sha>`.
2. **Verify** whether the change is needed (see Pre-port section above).
3. **Map** the Go code to Java equivalents — see [references/go-to-java-idioms.md](references/go-to-java-idioms.md). The most common mappings:
   - `func() error` closure → `SqlWriter` lambda (single-method functional interface)
   - `fmt.Errorf("%w: ...", ErrFoo)` sentinel error → `new ConversionException(userMsg, details)`
   - `errors.Is(err, ErrFoo)` → match `userMessage` or compare via subclass (rare)
   - struct + interface → final class implementing `Dialect` interface
4. **Test** — port the upstream test case, assert the same generated SQL. If the upstream test uses cel-go features cel-java lacks (most common: `format()` from the strings extension), declare a custom `CelFunctionDecl` in the test compiler.
5. **Document** — if behaviour differs from upstream by design, update the "Differences from upstream `cel2sql` (Go)" section in `CLAUDE.md`.

## When to split into two PRs

Big upstream syncs often bundle multiple themes. The v3.7.0 + v3.7.1 sync (which became cel2sql4j PRs #9 and #10) followed this shape:

- **PR A (small)**: bug fixes + new options + small docs. ~300–500 LOC. Quick to review.
- **PR B (large)**: new dialect or substantial new feature. ~1000+ LOC. Reviewed carefully.

See [references/two-pr-shape.md](references/two-pr-shape.md) for the heuristics — when to split, what to bundle in each PR, how to order the merges.

## Scripts

- **Run** `.claude/skills/port-from-upstream/scripts/list_upstream_changes.sh [<since-sha>]` — lists upstream commits and tags. Default `<since>` is auto-detected from the most recent "Port upstream..." commit on the current cel2sql4j branch. Fails clearly if the upstream repo isn't checked out at the expected path.

## References

- [references/go-to-java-idioms.md](references/go-to-java-idioms.md) — the Go-to-Java mapping table (closures, errors, struct tags, generics, reflection).
- [references/two-pr-shape.md](references/two-pr-shape.md) — when and how to split an upstream sync into two PRs.
