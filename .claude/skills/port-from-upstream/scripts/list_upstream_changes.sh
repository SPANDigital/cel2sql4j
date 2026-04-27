#!/usr/bin/env bash
# List candidate upstream commits + recent tags for porting from
# github.com/spandigital/cel2sql into cel2sql4j.
#
# Usage:
#   list_upstream_changes.sh [<since-sha>]
#
# If <since-sha> is omitted, the script auto-detects it from the most recent
# "Port upstream..." commit on the current cel2sql4j branch (looks at the
# subject line). If no such commit is found, it falls back to the last 30
# commits.
#
# The script does NOT modify the upstream repo; it only reads from it.
#
# Override the upstream path with UPSTREAM_REPO=/path/to/cel2sql.
set -euo pipefail

UPSTREAM_REPO="${UPSTREAM_REPO:-/Users/richardwooding/Code/SPAN/cel2sql}"

if [ ! -d "$UPSTREAM_REPO/.git" ]; then
  echo "Error: upstream repo not found at $UPSTREAM_REPO" >&2
  echo "Set UPSTREAM_REPO=<path> if it's checked out elsewhere." >&2
  echo "Or clone it: git clone https://github.com/spandigital/cel2sql $UPSTREAM_REPO" >&2
  exit 2
fi

# Resolve since-sha: arg, then auto-detect, then fallback.
SINCE="${1:-}"
if [ -z "$SINCE" ]; then
  # Find the last cel2sql4j commit that mentions "Port upstream" in its subject,
  # extract the upstream SHA from its message body if present (typical format:
  # "Backports upstream commit <sha>" or "Mirrors upstream <sha>").
  CEL2SQL4J_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
  if [ -n "$CEL2SQL4J_ROOT" ]; then
    SINCE="$(git -C "$CEL2SQL4J_ROOT" log --grep='[Pp]ort.*upstream\|[Bb]ackport' -1 --pretty=format:%B \
              | grep -oE '[0-9a-f]{7,40}' | head -1 || true)"
  fi
fi

echo "=== Upstream repo: $UPSTREAM_REPO ==="
echo

echo "=== Recent tags (10 newest) ==="
git -C "$UPSTREAM_REPO" tag --sort=-creatordate | head -10
echo

if [ -n "$SINCE" ] && git -C "$UPSTREAM_REPO" cat-file -e "$SINCE" 2>/dev/null; then
  echo "=== Commits since $SINCE ==="
  git -C "$UPSTREAM_REPO" log --oneline "$SINCE..HEAD"
else
  if [ -n "$SINCE" ]; then
    echo "Note: '$SINCE' not found in upstream; showing last 30 commits instead." >&2
  else
    echo "Note: no since-sha auto-detected from cel2sql4j git log; showing last 30 commits." >&2
  fi
  echo
  echo "=== Last 30 upstream commits ==="
  git -C "$UPSTREAM_REPO" log --oneline -30
fi

echo
echo "=== Hints ==="
echo "- Use 'git -C $UPSTREAM_REPO show <sha>' to read a commit's diff."
echo "- Use 'git -C $UPSTREAM_REPO log --grep=<keyword>' to find commits by message."
echo "- Many upstream fixes are already done in cel2sql4j — grep cel2sql4j first."
echo "  See SKILL.md 'Pre-port verification' section."
