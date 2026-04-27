# When to Split into Two PRs

Big upstream syncs (e.g. v3.7.0 + v3.7.1 → 1300+ LOC delta) review better as two PRs than one. This is the heuristic.

## Contents

- The core split rule
- Worked example: PR #9 + PR #10 (Apr 2026)
- Order of merge
- When NOT to split

---

## The core split rule

Split into two PRs when **one PR contains a large new dialect / new module** (~1000+ LOC, mostly new files) and **the other contains many small surgical changes** (bug fixes, new options, doc updates).

The two have very different review modes:

- **Surgical PR**: reviewer scans diffs line by line. Wants to see test coverage for each change. Reads CLAUDE.md updates carefully.
- **Module PR**: reviewer reads `<Module>Dialect.java` top-to-bottom as a self-contained file. Cross-references against an existing dialect (DuckDB, etc.) for shape. Doesn't re-review every line.

Forcing both into one PR makes both modes harder.

## Worked example: PR #9 + PR #10

The cel2sql v3.7.0 + v3.7.1 sync split as:

**PR #9** (small, ~370 LOC new + ~13 modified):

- New `ConvertOptions`: `withJsonVariables`, `withColumnAliases`, `withParamStartIndex`
- Inline byte-literal cap (10 000 bytes, CWE-400)
- BigQuery `ARRAY_LENGTH` COALESCE wrap fix
- New CEL `format()` function across 5 of 6 dialects (MySQL throws)
- 21 new unit tests; README + CLAUDE.md updates for the small additions

**PR #10** (large, ~700 LOC new):

- New `dialect/spark/` package: `SparkDialect`, `SparkValidation`, `SparkRegex`
- `DialectName.SPARK` enum constant
- 18 unit tests in a new `Cel2SqlSparkTest`
- README badge + dialect table row; CLAUDE.md dialect table column

Both branched off the same baseline (`main` at the time PR A was opened). PR #10 was off `main` rather than off PR #9 because Spark didn't depend on PR #9's changes.

## Order of merge

Always merge the small PR first, then the large one:

1. PR A (small) merges → main updated.
2. Rebase PR B (large) onto the new main if conflicts arise.
3. Merge PR B.

Reviewers prefer this order because PR A's changes ride into the integration tests on PR B's CI run.

## Bundling guidelines

| Item | Goes with PR A (small) | Goes with PR B (large) |
|---|---|---|
| Bug fix touching 1–2 files | ✓ | |
| New option on `ConvertOptions` | ✓ | |
| New CEL function (touches `Converter` + 6 dialects + tests) | ✓ (it's mechanical, not a module) | |
| New SQL dialect (whole package + tests + docs) | | ✓ |
| New CEL extension library wired in | | ✓ |
| README/CLAUDE.md updates | both — split docs by which PR they describe | both |

## When NOT to split

- **Total diff is small.** Under ~400 LOC combined, one PR is fine.
- **The "new module" depends on the surgical changes.** If PR B couldn't compile without PR A's interface widening, they belong together — or PR A widens the interface and PR B uses it (still split, but ordering matters).
- **The user explicitly asks for one PR.** Defer to them; bundling is fine if the maintainer prefers it.

## Communicating the split

Mention it in the plan and in the PR descriptions:

- PR A description: "Branched from main; PR B (Spark dialect, ~700 LOC) is a follow-up off this same baseline."
- PR B description: "Branched from main (independent of PR #9); will rebase onto PR #9 if it merges first."

Reviewers should never have to guess which PR landed first.
