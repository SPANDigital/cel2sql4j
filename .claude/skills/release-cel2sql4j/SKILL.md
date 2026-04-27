---
name: release-cel2sql4j
description: Cuts a new cel2sql4j release by validating the working tree, picking the semver bump, tagging `vX.Y.Z`, and pushing the tag to trigger `.github/workflows/release.yml` which runs tests and publishes to Maven Central via the vanniktech publish plugin. Use when shipping a new patch, minor, or major version of cel2sql4j to Maven Central.
---

# Release cel2sql4j

cel2sql4j ships to Maven Central whenever a `v*` tag lands on `main`. The release flow is shorter than upstream cel2sql's because there's no CHANGELOG to maintain — `release.yml` builds release notes automatically from merged PR titles via `softprops/action-gh-release@v2` with `generate_release_notes: true`.

## Quick start

```bash
git checkout main && git pull --ff-only origin main

# 1. Run preflight checks (working tree clean, CI green, list candidate commits).
.claude/skills/release-cel2sql4j/scripts/release_preflight.sh

# 2. Decide the version (see "Picking the version" below).
VERSION=v0.2.0

# 3. Tag annotated and push — the tag push is what triggers release.yml.
git tag -a "$VERSION" -m "Release $VERSION"
git push origin "$VERSION"

# 4. Watch the release workflow.
gh run list --workflow Release --limit 1
gh run watch
```

The workflow:

1. Validates the tag format (`vX.Y.Z` or `vX.Y.Z-qualifier`).
2. Runs unit tests on JDK 17.
3. Runs `./gradlew publishAndReleaseToMavenCentral` — the vanniktech plugin uploads, signs (GPG), and releases the staging repo on Sonatype Central Portal.
4. Creates a GitHub release with auto-generated notes. Tags with a hyphen (e.g. `v1.0.0-rc1`) are marked as `prerelease: true`.

The artifact lands at `https://central.sonatype.com/artifact/com.spandigital/cel2sql4j` once the workflow succeeds.

## Picking the version

cel2sql4j follows plain semver (no SIV — Java doesn't enforce module-path versioning the way Go does). Quick rules:

- **Patch (`v0.1.1`)** — Dependabot bumps without behaviour change, doc-only fixes, test fixes, internal refactors with identical generated SQL.
- **Minor (`v0.2.0`)** — new `ConvertOptions` field, new dialect, new CEL function, expanded surface in `Cel2Sql` / `Dialect`. The most common bump.
- **Major (`v1.0.0`)** — removal of an exported method, change to an existing method's signature, change to default behaviour that breaks callers (e.g. removing the byte-array cap, changing default dialect). **Reserve for genuinely user-disruptive breakage.**

When unsure between patch and minor: any new exported API surface bumps minor.

## Pre-1.0 caveat

Today the project is at `v0.1.0`. While in the 0.x line, breaking changes can land in a minor version (`0.1` → `0.2`) per semver convention. Once `v1.0.0` ships, breaking changes require a major bump.

## Common slip-ups

- **Lightweight tag instead of annotated.** Use `git tag -a vX.Y.Z -m "Release vX.Y.Z"`, not `git tag vX.Y.Z`. The release workflow doesn't strictly require annotation today, but auto-generated release notes look cleaner with one and amending later is awkward.
- **Stale `gradle.properties` version.** `VERSION_NAME=0.0.1-SNAPSHOT` in `gradle.properties` is only used for local snapshot builds — `release.yml` overrides it via `ORG_GRADLE_PROJECT_VERSION_NAME=$TAG`. **Don't update `gradle.properties` as part of the release.** It can drift; that's by design.
- **Pre-release qualifier shape.** Use `v1.0.0-rc1`, `v1.0.0-beta.1`, `v1.0.0-rc.2`. The validator accepts `^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$` after stripping the leading `v`. Underscores are rejected.
- **Forgetting to merge open security PRs first.** Dependabot security advisories that are merge-ready should land before the tag — the release otherwise ships the known-vulnerable transitive deps. The preflight script flags any open Dependabot PRs.
- **Tagging from a feature branch.** The tag must point at a commit reachable from `main`. The preflight script checks this.

## Required secrets (org-level)

The release workflow reads four secrets:

| Secret | Purpose |
|---|---|
| `MAVEN_CENTRAL_USERNAME` | Sonatype Central Portal token username |
| `MAVEN_CENTRAL_PASSWORD` | Sonatype Central Portal token password |
| `GPG_SIGNING_KEY` | ASCII-armored GPG private key — **must be RSA**, not Ed25519 (Gradle's Bouncy Castle doesn't support newer key types) |
| `GPG_SIGNING_PASSWORD` | Passphrase for the GPG key |

If a release workflow fails with a Sonatype 401 or a GPG error, the secrets need rotating; that's an org-admin task, not a release-skill task.

## After the tag pushes

Once the workflow goes green:

1. The GitHub release at `https://github.com/SPANDigital/cel2sql4j/releases/tag/<tag>` is created with auto-generated notes (from PR titles since the previous tag).
2. The artifact appears on Maven Central within ~30 minutes — sometimes longer.
3. If the auto-generated notes need editing (e.g. group changes by category), use `gh release edit <tag> --notes-file <path>` — the skill considers post-edit polish optional.

## Verification checklist

After pushing the tag:

- [ ] Release workflow succeeded — `gh run list --workflow Release --limit 1`.
- [ ] GitHub release exists at the expected URL.
- [ ] `mvn dependency:get -Dartifact=com.spandigital:cel2sql4j:<version>` resolves (this can take ~30 min — sync delay).
- [ ] README's Maven Central badge updates to the new version.

If the workflow failed mid-publish, the staging repo on Sonatype may be left in a half-released state — the operator may need to drop it via the [Central Portal UI](https://central.sonatype.com/publishing/deployments) before retrying. Don't simply re-tag with the same version; bump the patch.

## Scripts

- **Run** `.claude/skills/release-cel2sql4j/scripts/release_preflight.sh [<version>]` — validates the working tree (clean, on `main`, in sync with origin), checks CI is green on `main`, lists open Dependabot PRs, prints the commit log since the previous tag (the release-notes preview), and — if `<version>` is supplied — validates the version-string format and prints the exact `git tag -a` / `git push` commands to run.

## References

The relevant files are small enough to read directly when needed (no separate references):

- `.github/workflows/release.yml` — the workflow definition.
- `gradle.properties` — `VERSION_NAME=...-SNAPSHOT` for local builds (overridden at release).
- `build.gradle.kts` — the `mavenPublishing { publishToMavenCentral(...) }` block.
- `CLAUDE.md` "CI/CD" section — same info in summarised form for general orientation.
