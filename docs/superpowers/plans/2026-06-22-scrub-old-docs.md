# Scrub Stale Superpowers Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A scheduled GitHub Action that deletes `docs/superpowers/{specs,plans}` markdown files older than 30 days (by their `YYYY-MM-DD` filename prefix) and opens a PR for review.

**Architecture:** A self-contained POSIX `sh` script (`scripts/scrub-old-docs.sh`) owns all decision/delete logic; age is computed with pure shell integer arithmetic (no `date -d`, so it runs identically on the macOS dev box and the Ubuntu runner). A thin workflow (`.github/workflows/scrub-old-docs.yml`) runs the script weekly, then commits the deletions to a stable branch and opens/updates a PR via the preinstalled `gh` CLI.

**Tech Stack:** POSIX shell, GitHub Actions, `git`, `gh` CLI. No new repo dependencies (nothing for `cargo deny`/`cargo machete` to see — these are non-Rust files).

## Global Constraints

- Scope is exactly `docs/superpowers/specs/*.md` and `docs/superpowers/plans/*.md`. Nothing else under `docs/` is touched.
- Age source is the leading `YYYY-MM-DD` filename prefix only — never git history or filesystem mtime.
- Threshold: delete when `age_days >= 30` (default; `--max-age-days` overrides).
- Fail-safe: a file with no valid leading `YYYY-MM-DD` (or an out-of-range month/day) is **skipped, never deleted**.
- The script must be pure POSIX `sh` (runs under Ubuntu `dash` and macOS `sh`); do **not** use GNU-only `date -d` for the age math.
- Deletions land via a PR into `main` (the review gate), never a direct push to `main`.
- Use only the built-in `GITHUB_TOKEN` — no PAT, no third-party actions.
- Commit author identity for any local commits in this worktree: `Francis Hwang <sera@fhwang.net>`. This is a **public repo** — keep personal/medical data out of commits (not a risk here, but the rule stands).

---

### Task 1: The scrub script

Builds `scripts/scrub-old-docs.sh` and verifies it against throwaway fixtures (dry-run path) and a throwaway git repo (real-delete path). There is no shell test framework in this repo and `just check` is Rust-only, so the tests below are scratch harnesses the implementer runs — they are **not** committed.

**Files:**
- Create: `scripts/scrub-old-docs.sh`
- Test (scratch, not committed): `/tmp/test-scrub-dryrun.sh`, `/tmp/test-scrub-delete.sh`

**Interfaces:**
- Consumes: nothing.
- Produces: an executable `scripts/scrub-old-docs.sh` with this CLI, relied on by Task 2's workflow:
  - `scrub-old-docs.sh [--max-age-days N] [--dry-run] [--today YYYY-MM-DD] [DIR ...]`
  - Defaults: `N=30`, today = `date -u +%Y-%m-%d`, `DIR` = `docs/superpowers/specs docs/superpowers/plans`.
  - `--dry-run` prints `would delete: <path> (<age>d old)` and changes nothing.
  - Real run prints `deleted: <path> (<age>d old)` and `git rm --quiet`s the file.
  - Prints `no stale docs found` when nothing matched. Always exits `0` on success.

- [ ] **Step 1: Write the failing dry-run test harness**

Create `/tmp/test-scrub-dryrun.sh` (run later from the repo root). It builds fixtures with a pinned reference date of `2026-06-22`, where `2026-05-23` is exactly 30 days old (delete) and `2026-05-24` is 29 days old (keep):

```sh
#!/usr/bin/env sh
set -eu
REPO=$(pwd)
T=$(mktemp -d)
mkdir -p "$T/specs" "$T/plans"
: > "$T/specs/2026-01-01-old-spec.md"      # ~172d  -> delete
: > "$T/specs/2026-05-23-thirty.md"        # 30d    -> delete (boundary, >=)
: > "$T/plans/2026-05-24-twentynine.md"    # 29d    -> keep
: > "$T/plans/2026-06-20-fresh.md"         # 2d     -> keep
: > "$T/specs/notes.md"                     # no prefix -> skip
: > "$T/plans/2026-13-99-bad.md"           # bad month/day -> skip

out=$(sh "$REPO/scripts/scrub-old-docs.sh" --dry-run --today 2026-06-22 "$T/specs" "$T/plans" 2>/dev/null)
echo "----- output -----"; echo "$out"; echo "------------------"

fail=0
echo "$out" | grep -q "old-spec.md"   || { echo "FAIL: old-spec not flagged"; fail=1; }
echo "$out" | grep -q "thirty.md"     || { echo "FAIL: 30d boundary not flagged"; fail=1; }
echo "$out" | grep -q "twentynine.md" && { echo "FAIL: 29d wrongly flagged"; fail=1; }
echo "$out" | grep -q "fresh.md"      && { echo "FAIL: fresh wrongly flagged"; fail=1; }
echo "$out" | grep -q "would delete:.*notes.md" && { echo "FAIL: no-prefix wrongly flagged"; fail=1; }
echo "$out" | grep -q "would delete:.*bad.md"   && { echo "FAIL: bad-date wrongly flagged"; fail=1; }
rm -rf "$T"
[ "$fail" -eq 0 ] && echo "ALL DRY-RUN TESTS PASSED" || { echo "DRY-RUN TESTS FAILED"; exit 1; }
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `sh /tmp/test-scrub-dryrun.sh`
Expected: FAIL — `scripts/scrub-old-docs.sh` does not exist yet (sh reports "No such file or directory").

- [ ] **Step 3: Write the script**

Create `scripts/scrub-old-docs.sh`:

```sh
#!/usr/bin/env sh
# Scrub stale superpowers design/plan docs by filename-date age.
# Deletes (git rm) markdown files under the target dirs whose leading
# YYYY-MM-DD filename prefix is at least MAX_AGE_DAYS old.
# Spec: docs/superpowers/specs/2026-06-22-scrub-old-docs-design.md
set -eu

max_age_days=30
dry_run=0
today=""

usage() {
  cat <<'EOF'
Usage: scrub-old-docs.sh [--max-age-days N] [--dry-run] [--today YYYY-MM-DD] [DIR ...]

Deletes markdown files whose leading YYYY-MM-DD filename prefix is at least
N days old (default 30). --dry-run prints what it would delete and changes
nothing. --today pins the reference date (default: UTC today). DIR defaults
to docs/superpowers/specs and docs/superpowers/plans.
EOF
}

# Days since 1970-01-01 for a proleptic Gregorian date (Hinnant's algorithm).
# Pure integer arithmetic so it needs no GNU `date -d`.
days_from_civil() {
  _y=$1; _m=$2; _d=$3
  _y=$(( _y - (_m <= 2) ))
  _era=$(( (_y >= 0 ? _y : _y - 399) / 400 ))
  _yoe=$(( _y - _era * 400 ))
  _doy=$(( (153 * (_m + (_m > 2 ? -3 : 9)) + 2) / 5 + _d - 1 ))
  _doe=$(( _yoe * 365 + _yoe / 4 - _yoe / 100 + _doy ))
  echo $(( _era * 146097 + _doe - 719468 ))
}

# Echo the epoch-day number for a string starting with YYYY-MM-DD, or
# return 1 if it has no valid leading date.
prefix_to_day() {
  case "$1" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*) ;;
    *) return 1 ;;
  esac
  _p=$(printf '%.10s' "$1")          # first 10 chars = YYYY-MM-DD
  _yr=${_p%%-*}; _rest=${_p#*-}; _mo=${_rest%%-*}; _dy=${_rest#*-}
  _mo=${_mo#0}; _dy=${_dy#0}          # strip the single possible leading zero
  [ "$_mo" -ge 1 ] && [ "$_mo" -le 12 ] || return 1
  [ "$_dy" -ge 1 ] && [ "$_dy" -le 31 ] || return 1
  days_from_civil "$_yr" "$_mo" "$_dy"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --max-age-days) max_age_days=$2; shift 2 ;;
    --dry-run) dry_run=1; shift ;;
    --today) today=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) break ;;
  esac
done

[ $# -eq 0 ] && set -- docs/superpowers/specs docs/superpowers/plans
[ -n "$today" ] || today=$(date -u +%Y-%m-%d)
today_day=$(prefix_to_day "$today") || { echo "invalid --today: $today" >&2; exit 2; }

deleted=0
for dir in "$@"; do
  [ -d "$dir" ] || continue
  for file in "$dir"/*.md; do
    [ -e "$file" ] || continue       # glob matched nothing
    base=${file##*/}
    if ! file_day=$(prefix_to_day "$base"); then
      echo "skip (no/invalid date prefix): $file" >&2
      continue
    fi
    age=$(( today_day - file_day ))
    if [ "$age" -ge "$max_age_days" ]; then
      if [ "$dry_run" -eq 1 ]; then
        echo "would delete: $file (${age}d old)"
      else
        git rm --quiet "$file"
        echo "deleted: $file (${age}d old)"
      fi
      deleted=$(( deleted + 1 ))
    fi
  done
done

[ "$deleted" -eq 0 ] && echo "no stale docs found"
exit 0
```

Then make it executable:

Run: `chmod +x scripts/scrub-old-docs.sh`

- [ ] **Step 4: Run the dry-run harness to verify it passes**

Run: `sh /tmp/test-scrub-dryrun.sh`
Expected: prints the six fixtures' verdicts and ends with `ALL DRY-RUN TESTS PASSED`.

- [ ] **Step 5: Write and run the real-delete test harness**

Create `/tmp/test-scrub-delete.sh` — exercises the `git rm` path in a throwaway repo:

```sh
#!/usr/bin/env sh
set -eu
SCRIPT=$(pwd)/scripts/scrub-old-docs.sh
G=$(mktemp -d); cd "$G"
git init -q; mkdir d
echo x > d/2026-01-01-old.md
echo x > d/2026-06-20-new.md
git add -A
git -c user.email=t@example.com -c user.name=t commit -qm init

sh "$SCRIPT" --today 2026-06-22 d >/dev/null

fail=0
[ -f d/2026-06-20-new.md ] || { echo "FAIL: fresh file removed"; fail=1; }
[ -f d/2026-01-01-old.md ] && { echo "FAIL: old file still on disk"; fail=1; }
git status --porcelain | grep -q '^D  d/2026-01-01-old.md' || { echo "FAIL: deletion not staged"; fail=1; }
cd /; rm -rf "$G"
[ "$fail" -eq 0 ] && echo "ALL DELETE TESTS PASSED" || { echo "DELETE TESTS FAILED"; exit 1; }
```

Run: `sh /tmp/test-scrub-delete.sh`
Expected: `ALL DELETE TESTS PASSED` (old file gone from disk and staged as `D`, fresh file untouched).

- [ ] **Step 6: Sanity-check against the real repo (no stale docs yet)**

The two real docs are dated `2026-06-17` and `2026-06-22` — both well under 30 days as of today, so a dry run must flag nothing.

Run: `sh scripts/scrub-old-docs.sh --dry-run`
Expected: `no stale docs found` (no `would delete:` lines).

- [ ] **Step 7: Commit**

```bash
git add scripts/scrub-old-docs.sh
git -c user.name='Francis Hwang' -c user.email='sera@fhwang.net' commit -m "Add scrub-old-docs script for age-based doc cleanup (#8)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The scheduled workflow

Wraps the script in a weekly GitHub Action that opens/updates a single review PR.

**Files:**
- Create: `.github/workflows/scrub-old-docs.yml`

**Interfaces:**
- Consumes: `scripts/scrub-old-docs.sh` from Task 1 (invoked as `sh scripts/scrub-old-docs.sh`).
- Produces: nothing other tasks depend on (terminal deliverable).

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/scrub-old-docs.yml`:

```yaml
name: Scrub old docs

on:
  schedule:
    - cron: '0 7 * * 1' # Mondays 07:00 UTC
  workflow_dispatch:

permissions:
  contents: write
  pull-requests: write

concurrency:
  group: scrub-old-docs
  cancel-in-progress: false

jobs:
  scrub:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5

      - name: Scrub stale docs
        run: sh scripts/scrub-old-docs.sh

      - name: Detect deletions
        id: diff
        run: |
          if [ -n "$(git status --porcelain)" ]; then
            echo "changed=true" >> "$GITHUB_OUTPUT"
          else
            echo "changed=false" >> "$GITHUB_OUTPUT"
            echo "Nothing to scrub."
          fi

      - name: Open or update scrub PR
        if: steps.diff.outputs.changed == 'true'
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          set -eu
          branch=chore/scrub-old-docs
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git checkout -B "$branch"
          git commit -m "chore: scrub stale superpowers docs"
          git push --force origin "$branch"
          if [ -z "$(gh pr list --head "$branch" --state open --json number --jq '.[].number')" ]; then
            gh pr create --head "$branch" --base main \
              --title "chore: scrub stale superpowers docs" \
              --body "Automated removal of \`docs/superpowers/{specs,plans}\` markdown older than 30 days (by filename date). Opened by the scrub-old-docs workflow."
          else
            echo "PR already open for $branch; force-push updated it in place."
          fi
```

Notes for the implementer:
- The `git rm` in the script stages the deletions; `git checkout -B` preserves that staged index across the branch creation, so `git commit` (no `-a` needed) captures exactly those deletions.
- `git push --force` is safe here: `chore/scrub-old-docs` is a bot-owned, single-purpose branch reset to `main` + one commit each run.
- A PR opened by `GITHUB_TOKEN` will not itself trigger `ci.yml`; that is expected and fine (deleting docs cannot affect `just check`).

- [ ] **Step 2: Validate the YAML parses**

Run: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/scrub-old-docs.yml')); print('yaml ok')"`
Expected: `yaml ok`

(If `actionlint` happens to be installed, also run `actionlint .github/workflows/scrub-old-docs.yml` and expect no output. Do not install it just for this — the authoritative validation is the first `workflow_dispatch` run after merge.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/scrub-old-docs.yml
git -c user.name='Francis Hwang' -c user.email='sera@fhwang.net' commit -m "Add weekly scrub-old-docs workflow (#8)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Post-implementation verification

After both tasks, confirm the deliverables exist and the script is clean:

- `sh scripts/scrub-old-docs.sh --dry-run` → `no stale docs found`.
- `git -c color.ui=never log --oneline -3` shows the two new commits plus the spec commit.
- The workflow's real behavior is verified by a manual **Run workflow** (`workflow_dispatch`) on the Actions tab once this is merged to `main`; the first Monday cron run confirms the schedule path.

Note: `just check` is **not** required for this change — it gates the Rust workspace, and these are non-Rust files (shell + YAML) it neither compiles nor lints. Running it is harmless but proves nothing about this work.

## Self-review notes

- **Spec coverage:** scope (Task 1 dirs/glob), filename-date age + 30d threshold + `>=` boundary (Task 1 Steps 1/3), fail-safe skip (Task 1 fixtures `notes.md`/`bad.md`), weekly cron + dispatch (Task 2), stable-branch dedup'd PR (Task 2 Step 1), `GITHUB_TOKEN`-only + no new deps (Global Constraints) — all mapped.
- **Portability deviation from spec:** the spec named GNU `date -d`; the plan uses pure-arithmetic `days_from_civil` instead. Same result, but POSIX-portable so it is testable on the macOS dev box. Within the spec's "runnable/testable by hand" intent.
- **Type/name consistency:** Task 2 invokes exactly the CLI Task 1 produces (`sh scripts/scrub-old-docs.sh`, default dirs, default 30d). Branch name `chore/scrub-old-docs` used consistently.
