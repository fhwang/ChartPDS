#!/bin/sh
# Print "<sha256>  <path>" for every git-tracked file under holdout/, sorted by
# path. This is the canonical content fingerprint of the holdout suite.
#
# Used by `just holdout-bless` (to (re)write holdout.lock) and by
# scripts/holdout-verify.sh (to compare the working tree against it). Keeping
# the enumeration in one place guarantees bless and verify agree.
set -eu

cd "$(git rev-parse --show-toplevel)"

if command -v sha256sum >/dev/null 2>&1; then
  hash_cmd="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  hash_cmd="shasum -a 256"
else
  echo "holdout-hashes: need sha256sum or shasum on PATH" >&2
  exit 1
fi

# git ls-files enumerates only tracked files, so build artifacts and untracked
# scratch never leak into the fingerprint. LC_ALL=C keeps the sort stable across
# platforms.
git ls-files holdout | LC_ALL=C sort | while IFS= read -r f; do
  h=$($hash_cmd "$f" | cut -d' ' -f1)
  printf '%s  %s\n' "$h" "$f"
done
