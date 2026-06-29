#!/bin/sh
# Verify the working tree's holdout/ contents match the committed holdout.lock.
#
# Fails loudly, naming the drift, if any protected holdout file was added,
# removed, or modified without a corresponding `just holdout-bless`. This is the
# content half of the holdout gate (the signature half lives in
# scripts/holdout-verify-signatures.sh). It runs locally via `just check` and in
# CI, so a weakened holdout test surfaces as one specific failure rather than a
# subtle line buried in a large diff.
set -eu

cd "$(git rev-parse --show-toplevel)"

if [ ! -f holdout.lock ]; then
  echo "holdout-verify: holdout.lock is missing; run 'just holdout-bless \"...\"' to create it" >&2
  exit 1
fi

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
scripts/holdout-hashes.sh >"$tmp"

if ! diff -u holdout.lock "$tmp" >/dev/null; then
  echo "ERROR: holdout/ contents do not match holdout.lock." >&2
  echo "A protected holdout file was added, removed, or modified." >&2
  echo "If this change is intentional, a human must run 'just holdout-bless \"<why>\"'." >&2
  echo >&2
  echo "--- holdout.lock (expected) vs working tree (actual) ---" >&2
  diff -u holdout.lock "$tmp" >&2 || true
  exit 1
fi

echo "holdout-verify: OK ($(wc -l <"$tmp" | tr -d ' ') protected files match holdout.lock)"
