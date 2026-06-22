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
