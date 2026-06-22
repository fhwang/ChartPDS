# Scrub stale superpowers docs (GitHub Action)

Resolves issue #8 ("GHA to scrub out old docs").

## Problem

The brainstorming and writing-plans skills deposit working artifacts under
`docs/superpowers/specs/` and `docs/superpowers/plans/`. These are transient
design/plan files; left alone they accumulate in the tree indefinitely. We want
an automated, age-based cleanup with a human review checkpoint.

## Goal

A scheduled GitHub Action that finds spec/plan markdown files older than a
threshold and opens a pull request deleting them. No direct-to-`main` writes —
the PR is the review gate, and git history preserves any deleted file.

## What counts as "stale"

- **Scope:** `docs/superpowers/specs/*.md` and `docs/superpowers/plans/*.md`
  only. Nothing else under `docs/` is touched.
- **Age source:** the `YYYY-MM-DD` filename prefix that the brainstorming and
  writing-plans skills always prepend (e.g. `2026-06-17-observation-aggregators.md`).
  No git history or filesystem mtime is consulted — both are unreliable in CI
  checkouts, and the prefix is the design's own creation date.
- **Threshold:** a file is stale when `today − prefix_date ≥ 30 days`.
- **Fail-safe:** a file whose name lacks a valid leading `YYYY-MM-DD` is skipped
  (never deleted). Likewise a prefix that does not parse as a real date is
  skipped.

The "edited an old doc yesterday" case still counts as old — age is the age of
the design, not the last edit. This is intentional.

## Components

Two pieces. The decision/delete logic lives in a checked-in shell script (so it
is reviewable and runnable by hand); the workflow stays a thin declarative
wrapper.

### `scripts/scrub-old-docs.sh`

POSIX `sh` script. Responsibilities:

- Glob `docs/superpowers/specs/*.md` and `docs/superpowers/plans/*.md`.
- For each file, extract a leading `YYYY-MM-DD` from the basename. Skip files
  with no valid prefix.
- Convert the prefix to an epoch day using GNU `date -d "$prefix" +%s` (the
  Ubuntu runner has GNU date). Skip if `date` rejects it.
- Compute age in whole days against "now" (`date +%s`). Delete with
  `git rm --quiet "$file"` when `age_days >= MAX_AGE_DAYS`.
- Flags:
  - `--max-age-days N` (default `30`).
  - `--dry-run` — print each file it *would* delete (and the reason), touch
    nothing, run no `git rm`.
- Exit 0 whether or not anything was deleted (nothing-to-do is success). Print a
  short summary line listing deletions (or "no stale docs found").

Determinism / testability: because age derives only from the filename and the
current date, the script is runnable locally —
`./scripts/scrub-old-docs.sh --dry-run` previews exactly what the workflow would
do.

### `.github/workflows/scrub-old-docs.yml`

- **Triggers:**
  - `schedule:` weekly — `cron: '0 7 * * 1'` (Mondays 07:00 UTC).
  - `workflow_dispatch:` — manual run from the Actions tab.
- **Permissions:** `contents: write`, `pull-requests: write` (built-in
  `GITHUB_TOKEN`; no PAT, no new third-party action).
- **Job steps:**
  1. `actions/checkout@v5`.
  2. Run `scripts/scrub-old-docs.sh` (real, not dry-run). It `git rm`s stale
     files into the working tree / index.
  3. If `git status --porcelain` is empty, log "nothing to scrub" and end the
     job successfully — no branch, no PR.
  4. Otherwise: configure a bot git identity, commit the deletions to the stable
     branch `chore/scrub-old-docs` (reset/force the branch to current `main`
     plus this commit so each run is a clean single-commit delta), and push.
  5. Open a PR from `chore/scrub-old-docs` into `main` via the preinstalled
     `gh` CLI — but only if no open PR already exists for that branch
     (`gh pr list --head chore/scrub-old-docs --state open`). If one is already
     open, the force-push in step 4 updates it in place; skip creating a
     duplicate.

## Behavior / trade-offs

- **PR, not direct commit.** Chosen for the review checkpoint. Deleted files
  remain recoverable from git history.
- **`GITHUB_TOKEN` limitation.** A PR opened by `GITHUB_TOKEN` does not itself
  trigger `ci.yml`. Acceptable here: deleting docs cannot affect `just check`.
  If CI-on-PR is ever wanted, swap in a PAT — out of scope now.
- **Stable branch, dedup'd PR.** A single long-lived `chore/scrub-old-docs`
  branch avoids piling up one open PR per week. Each run rebases it onto current
  `main`, so a merged-then-reopened cycle stays clean.
- **No new dependencies.** Uses only `git`, GNU `date`, and `gh` — all present
  on `ubuntu-latest`. Consistent with the repo's `cargo deny` / `cargo machete`
  dependency discipline.

## Testing

- **Local manual:** create throwaway fixtures with old and recent date prefixes
  in a scratch dir, point the script at them, confirm `--dry-run` flags exactly
  the old ones and a real run `git rm`s exactly those. Confirm a file with no
  valid prefix is left untouched, and a malformed-date prefix is skipped.
- **Boundary:** a file dated exactly `MAX_AGE_DAYS` ago is deleted (`>=`), one
  dated `MAX_AGE_DAYS − 1` is kept.
- **Workflow:** validated via a manual `workflow_dispatch` run after merge; the
  first scheduled run confirms the cron path.

## Out of scope

- Merge-status awareness (deleting only docs whose feature has merged).
- Archiving deleted docs elsewhere instead of relying on git history.
- Git-date or mtime-based age.
- A `just` recipe wrapper for the script (can be added later if it earns its
  keep).
