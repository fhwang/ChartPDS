#!/bin/sh
# CI-only: verify that every commit in BASE..HEAD which touches a PROTECTED path
# is signed by a key listed in .github/allowed_signers.
#
# Protected paths (a change to any of these requires a human-signed bless
# commit): the holdout suite itself, its lockfile, the allowed-signers roster,
# and this guard's own workflow. Folding the guard's files into the protected
# set means an agent cannot quietly disable the gate without tripping it.
#
# Commits that touch only crates/** and other unprotected paths flow through
# unsigned at full speed; only protected-path commits need a signature.
#
# Usage: holdout-verify-signatures.sh <base-ref> <head-ref>
set -eu

cd "$(git rev-parse --show-toplevel)"

BASE="${1:?usage: holdout-verify-signatures.sh <base-ref> <head-ref>}"
HEAD="${2:?usage: holdout-verify-signatures.sh <base-ref> <head-ref>}"

# Trust the committed roster for SSH signature verification.
git config gpg.ssh.allowedSignersFile .github/allowed_signers

protected_re='^(holdout/|holdout\.lock$|\.github/allowed_signers$|\.github/workflows/holdout\.yml$)'

fail=0
checked=0
for sha in $(git rev-list "$BASE".."$HEAD"); do
  if git diff-tree --no-commit-id --name-only -r "$sha" | grep -Eq "$protected_re"; then
    checked=$((checked + 1))
    if git verify-commit "$sha" >/dev/null 2>&1; then
      echo "ok: $sha is signed by an allowed key (touches a protected path)"
    else
      echo "ERROR: commit $sha touches a protected holdout path but is NOT signed by an allowed key" >&2
      git diff-tree --no-commit-id --name-only -r "$sha" | grep -E "$protected_re" | sed 's/^/    protected: /' >&2
      fail=1
    fi
  fi
done

if [ "$fail" -ne 0 ]; then
  echo >&2
  echo "Protected holdout paths may only change in a commit signed by a human" >&2
  echo "(run 'just holdout-bless \"<why>\"'). See the holdout design spec." >&2
  exit 1
fi

echo "holdout signature check OK ($checked protected-path commit(s) verified)"
