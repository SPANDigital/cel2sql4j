#!/usr/bin/env bash
# Scaffold a new SQL dialect by copying an existing one and renaming classes.
#
# Usage:
#   scaffold_dialect.sh <template> <new-name-lc> <NewClassPrefix>
#
# Example:
#   scaffold_dialect.sh duckdb cockroach Cockroach
#
# Args:
#   template:         existing dialect dir name under src/.../dialect/ (postgres|mysql|sqlite|duckdb|bigquery|spark)
#   new-name-lc:      new dialect dir name (lowercase, used as package name and DialectName value)
#   NewClassPrefix:   class-name prefix (e.g. Cockroach -> CockroachDialect, CockroachValidation, CockroachRegex)
#
# Output:
#   Lists every file created, plus next-step reminders.
#
# Exits non-zero if args are missing, the template doesn't exist, or the new
# package already exists. Creates files only — does not modify DialectName,
# tests, or docs (the agent does that next).
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <template> <new-name-lc> <NewClassPrefix>" >&2
  echo "Example: $0 duckdb cockroach Cockroach" >&2
  exit 2
fi

TEMPLATE="$1"
NEW_NAME="$2"
NEW_PREFIX="$3"

# Locate the repo root by walking up from this script until we find settings.gradle.kts.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
while [ "$REPO_ROOT" != "/" ] && [ ! -f "$REPO_ROOT/settings.gradle.kts" ]; do
  REPO_ROOT="$(dirname "$REPO_ROOT")"
done
if [ ! -f "$REPO_ROOT/settings.gradle.kts" ]; then
  echo "Error: could not find repo root (no settings.gradle.kts on the path up from $SCRIPT_DIR)" >&2
  exit 3
fi

DIALECT_DIR="$REPO_ROOT/src/main/java/com/spandigital/cel2sql/dialect"
TEMPLATE_DIR="$DIALECT_DIR/$TEMPLATE"
NEW_DIR="$DIALECT_DIR/$NEW_NAME"

if [ ! -d "$TEMPLATE_DIR" ]; then
  echo "Error: template dialect '$TEMPLATE' does not exist at $TEMPLATE_DIR" >&2
  echo "Available: $(ls "$DIALECT_DIR" | grep -v '\.java$' | tr '\n' ' ')" >&2
  exit 4
fi
if [ -e "$NEW_DIR" ]; then
  echo "Error: target directory $NEW_DIR already exists; refusing to overwrite" >&2
  exit 5
fi

# Derive the template's class-name prefix by reading any *Dialect.java filename.
TEMPLATE_PREFIX="$(basename "$(ls "$TEMPLATE_DIR"/*Dialect.java | head -1)" .java | sed 's/Dialect$//')"
TEMPLATE_NAME_LC="$TEMPLATE"
TEMPLATE_NAME_UC="$(echo "$TEMPLATE" | tr '[:lower:]' '[:upper:]')"
NEW_NAME_LC="$NEW_NAME"
NEW_NAME_UC="$(echo "$NEW_NAME" | tr '[:lower:]' '[:upper:]')"

mkdir -p "$NEW_DIR"

CREATED=()
for src in "$TEMPLATE_DIR"/*.java; do
  base="$(basename "$src")"
  new_base="${base/$TEMPLATE_PREFIX/$NEW_PREFIX}"
  dst="$NEW_DIR/$new_base"
  # Rename: package, class names, and the lowercase/uppercase dialect identifiers.
  sed \
    -e "s/dialect\.$TEMPLATE_NAME_LC/dialect.$NEW_NAME_LC/g" \
    -e "s/$TEMPLATE_PREFIX/$NEW_PREFIX/g" \
    -e "s/DialectName\.$TEMPLATE_NAME_UC/DialectName.$NEW_NAME_UC/g" \
    -e "s/\"$TEMPLATE_NAME_LC\"/\"$NEW_NAME_LC\"/g" \
    "$src" > "$dst"
  CREATED+=("$dst")
done

echo "Created ${#CREATED[@]} file(s):"
for f in "${CREATED[@]}"; do
  echo "  $f"
done
echo
echo "Next steps:"
echo "  1. Add ${NEW_NAME_UC}(\"${NEW_NAME_LC}\") to DialectName.java"
echo "  2. Edit $NEW_DIR/${NEW_PREFIX}Dialect.java — fill in dialect-specific SQL"
echo "  3. Edit $NEW_DIR/${NEW_PREFIX}Validation.java — reserved keywords + identifier shape"
echo "  4. Edit $NEW_DIR/${NEW_PREFIX}Regex.java — adjust per the dialect's regex engine (or delete if unsupported)"
echo "  5. Add ${NEW_NAME_UC} cases to test classes — see references/test-files.md"
echo "  6. Update README.md (badge grid + dialect table) and CLAUDE.md (dialect table)"
echo "  7. ./gradlew build"
