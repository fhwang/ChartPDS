#!/usr/bin/env bash
# Forbid junk parameter-object types. See "No junk parameter types" in
# CLAUDE.md for the policy and the refactoring playbook.
#
# Scope is crates/ only: holdout/ is a protected path and is checked by its
# own lockfile gate, not by style rules.
set -euo pipefail

cd "$(dirname "$0")/.."

fail=0

# *Params and *Options structs are forbidden everywhere: those names describe
# a function signature, not a domain concept.
if grep -rEn --include='*.rs' 'struct [A-Za-z0-9_]+(Params|Options)\b' crates; then
    fail=1
fi

# *Args structs are the MCP tool-argument convention (rmcp `Parameters<T>`
# needs a Deserialize + JsonSchema type per tool). They are wire-boundary
# DTOs, allowed only in chartpds-mcp.
if grep -rEn --include='*.rs' 'struct [A-Za-z0-9_]+Args\b' crates/chartpds-core; then
    fail=1
fi

if [ "$fail" -ne 0 ]; then
    cat >&2 <<'EOF'

error: junk parameter-object type(s) found (listed above).

A struct named *Params/*Options (or *Args outside chartpds-mcp) describes a
function signature, not the business. Do not rename it to a synonym like
*Fields, *Input, or *Data — model the domain instead. See "No junk parameter
types" in CLAUDE.md for the refactoring playbook.
EOF
    exit 1
fi
