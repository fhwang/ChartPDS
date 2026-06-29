# Holdout regression suite (agent-tamper-resistant tests)

## Problem

Changes land in large, fast-moving PRs that are coding-agent-authored. The
historical defense against regressions — unit tests — no longer holds, because
the same agent that writes the fix can also delete, `#[ignore]`, or silently
weaken a test (`assert_eq!(x, 5)` → `assert!(x >= 0)`) to reach green CI. In a
2000-line diff a human reviewer can miss it.

We want a small set of **holdout regression tests** that the agent cannot change
as part of normal fast-and-loose work. Each captures a real bug we never want to
see again. The agent may *draft* a holdout test on request, but the act of
admitting one into the protected set requires a deliberate human signature.
Below that line, code fixes can move as fast and sloppily as the agent likes.

## Threat model

The adversary is a **careless** agent, not an adversarial one: it will weaken a
test if that is the path of least resistance to green CI, but it is not actively
trying to deceive a watching human. The required property is therefore a
**CI-visible gate** — any change to the protected set produces a loud, specific
CI failure that cannot be made green by the agent. Everything stays in one repo.

This is explicitly *not* designed to hold against an agent that actively routes
around the mechanism (that would require a separate repo / external CI). If the
threat model escalates, the black-box test layer chosen here is the same one a
separate-repo design would use, so this can graduate without rewriting the tests.

## Two properties, kept separate

1. **Tamper-resistance** — the agent cannot quietly weaken a holdout test. This
   is a *gate*: who/what authorizes a change to the protected set.
2. **Decoupling** — holdout tests bind to a *stable* interface so that fast
   internal refactors do not *legitimately* break them. Without this, agents get
   a constant legitimate reason to touch the suite, creating re-bless churn that
   is itself cover for sneaky weakening.

The internal Rust API in this repo churns constantly (`list_problems` →
`current_problems`, `in_range` → `observation_history`, `list_medications` →
`current_medications`). The stable contract is the **13 MCP tools** — the actual
product surface. So holdout tests bind there, black-box.

## Prior art / where this sits relative to common practice

The mainstream playbook for "stop coding agents weakening tests" has converged on
three things, and this design adopts the first two and goes one notch harder on
the third:

- **Black-box / end-to-end tests as the load-bearing signal.** The widely-cited
  observation is that "code with no test and no mention is, to an AI agent,
  indistinguishable from dead code" — agents preserve what is tested at the
  product surface and silently erode what is not. This is exactly why holdout
  tests bind to the MCP tool surface, not internal functions. (Adopted.)
- **A three-tier hierarchy of authority** — Spec = law, pre-existing Tests =
  read-only verification, Code = the only mutable tier — enforced via agent
  instructions, with a "stop and report" rule when a protected test fails.
  (Adopted; see §6, CLAUDE.md.)
- **Soft enforcement** (prompts, CODEOWNERS, local pre-commit hooks). The field
  largely stops here. All three are bypassable by a careless agent chasing green
  CI: prompts can be ignored, CODEOWNERS only fires if a human actually catches
  the change in review (the precise failure mode on large PRs), and pre-commit
  hooks are `--no-verify`-skippable and not even installed in the agent's
  environment. This design replaces soft enforcement with the **signed-commit
  bless gate** (§3–§4) — bypass-proof because the agent physically cannot
  produce the signature. This is deliberately beyond mainstream practice; it is
  the only layer that closes the large-PR hole the soft gates leave open.

One caveat the research surfaces and this design heeds: blanket coverage can
*fossilize bad patterns* (the next agent extends whatever the tests lock in). So
the holdout set is kept **small and curated** — one entry per real bug we never
want back, not a coverage dragnet. The signed-bless friction is a feature here:
it makes adding a holdout test deliberate, which naturally keeps the set lean.

## Design

### 1. Location & format

A new top-level workspace member, `holdout/`, holding **black-box integration
tests** that:

- spawn the `chartpds-mcp` binary over stdio with a temp `CHARTPDS_DATA_DIR`,
- ingest synthetic CCDA fixtures (synthetic only — satisfies the
  no-private-data-in-public-repo rule),
- call real MCP tools (`ingest_record`, `list_problems`,
  `get_observation_history`, …),
- assert on the returned JSON.

It is a normal workspace member, so `cargo test --workspace` (hence `just check`)
runs it. This is deliberate: the agent *should* see a holdout test go red — that
is the regression signal — it simply cannot edit the test to make it green. The
harness (`holdout/src/lib.rs`) spawns the binary and drives it with `rmcp`'s
client transport (`TokioChildProcess` + `serve_client`), exposing a
`call_tool(name, args) -> Value` helper and an `ingest_ccda` convenience. Using
the same MCP client library as the wire protocol keeps the handshake robust
rather than hand-framed. To keep the crate's dependency footprint minimal — and
specifically to avoid pulling `tempfile`→`getrandom` into the non-dev dependency
graph, which trips the workspace `cargo deny` duplicate-version ban — the harness
uses a tiny std-only `TempDir` instead of the `tempfile` crate.

Fixtures (synthetic CCDA documents and any seed data) live under `holdout/` and
are themselves protected — a weakened fixture is a weakened test.

### 2. Tamper gate — three independent CI failures, none green-able by the agent

**a. Lockfile.** `holdout.lock` at the repo root contains the SHA-256 of every
file under `holdout/`. A `just`-driven check recomputes the hashes and fails on
any mismatch — content change, deletion, or rename. This converts "subtle
weakening buried in a large diff" into "one glaring failure that names the file."

**b. Path-scoped signature check.** On a PR, CI walks the PR's own commits
(`merge-base(origin/main, HEAD)..HEAD`) and, for every commit that touches a
**protected path**, requires a valid signature from a key listed in
`.github/allowed_signers` (verified with `git verify-commit`). Protected paths:

- `holdout/**`
- `holdout.lock`
- `.github/allowed_signers`
- `.github/workflows/holdout.yml` (the guard protects itself)

Commits that touch only `crates/**` etc. need no signature and flow through
unsigned at full agent speed.

**c. The suite runs.** The holdout integration tests execute and must pass.

A deletion of a holdout file trips both (a) and (b). A mid-fix weakening commit
that touches `holdout/` is unsigned on a protected path → (b) fails. The agent
cannot satisfy (b) because signing requires a physical act it cannot perform
(below).

### 3. Key custody — what makes the signature unforgeable by the agent

The signature is only a human gate if the key **cannot be used by the agent's
process without a physical human action**. A signing key cached unlocked on disk
defeats the entire mechanism, because the agent's `git commit -S` would sign
transparently.

Custody mechanism: an **SSH signing key in the macOS Secure Enclave, gated by
Touch ID** (via `secretive`, or `ssh-keygen -t ecdsa-sk`). Every bless requires
a fingerprint tap the agent cannot fake. Frictionless, native to the dev Mac, no
passphrase to manage or leak. The public half is registered as a *signing* key
and listed in `.github/allowed_signers`.

Normal commits stay **unsigned**. We require signatures *only* on protected
paths. A useful side effect: since nothing else ever signs, a Touch ID prompt
appearing during ordinary agent work is an unmistakable red flag.

### 4. Bless workflow

`just holdout-bless`:
1. regenerates `holdout.lock` from the current `holdout/` tree,
2. `git add` the holdout changes + lockfile,
3. `git commit -S` → Touch ID prompt → signed commit.

Only the human can complete step 3. CLAUDE.md instructs the agent: **never edit
`holdout/`, never run `holdout-bless`.** On request it may *draft* a holdout
test, but it must leave the changes staged-and-uncommitted and hand off with a
"ready to bless" message.

The intended authoring loop is **TDD at the holdout layer**:

- The human (agent-assisted for drafting) creates the *failing* regression test
  and commits it as a single **signed** commit — the deliberate moment.
- The agent then chews on the fix across many unsigned commits, refactoring
  freely. The only protected-path commit is the signed one; everything else is
  unsigned and unrestricted.
- Squash-merge collapses the branch into one commit on `main`. The signature is
  not preserved on `main`, and that is fine: the signature check runs on the
  PR's *pre-squash* commits, and branch protection blocks the merge while the
  check is red. The gate does its job before the squash.

If a later agent commit also touches `holdout/` (e.g. it tries to weaken the
fresh test), that commit is unsigned on a protected path → CI red. The holdout
files are frozen the instant they are signed.

### 5. Off-repo anchor

GitHub **branch protection** requires the holdout CI check to pass before a PR
can merge to `main`. This lives in repo *settings*, not in tracked files, so the
agent cannot touch it. It is what makes the in-repo gate unbypassable: even if
the agent deletes or neuters `.github/workflows/holdout.yml`, that edit is a
protected-path change (→ unsigned → would fail), and a missing required check
blocks the merge outright.

## Components

- `holdout/` — workspace member: stdio/JSON-RPC harness, synthetic CCDA
  fixtures, and the black-box regression tests.
- `holdout.lock` — root file, SHA-256 of every `holdout/**` file.
- `.github/allowed_signers` — the human's allowed signing key(s).
- `.github/workflows/holdout.yml` — CI: lockfile verify, path-scoped signature
  verify, run the holdout suite.
- `scripts/holdout-verify.sh` — the decision logic (lockfile recompute + commit
  signature walk), checked in so it is reviewable and runnable by hand; the
  workflow is a thin wrapper, mirroring `scrub-old-docs`.
- `Justfile` recipes: `holdout-bless` (regenerate lock + signed commit) and
  `holdout-verify` (run the same check CI runs, minus the signature step which
  needs the PR range).
- `CLAUDE.md` addition: a **three-tier authority** rule — the spec/holdout tests
  are read-only to the agent; only `crates/**` is freely mutable — plus the
  concrete operating rules: never edit `holdout/`, never run `holdout-bless`; on
  request, *draft* a holdout test but leave it staged-and-uncommitted and hand
  off; and when a holdout test fails, **stop and report** rather than touch the
  test. The draft-and-hand-off workflow is the only sanctioned way holdout files
  change.

## Behavior / trade-offs

- **Holdout tests run in `just check`.** The agent gets the red signal but
  cannot act on it except by fixing the actual code. Intended.
- **Squash-merge erases the signature from `main`.** Acceptable — the gate is
  pre-merge, enforced by branch protection. If signatures-on-`main` are ever
  wanted, switch to merge-commit or rebase-merge.
- **Residual risk (named honestly):** an agent-initiated `git commit -S` pops a
  Touch ID prompt on the human's Mac; a habituated tap would approve it.
  Blunted by: normal commits are unsigned (any prompt is anomalous), and the
  convention that bless happens only in a separate human-opened terminal.
  Sufficient for the careless threat model; not claimed to be more.
- **No new runtime dependencies.** The harness uses the existing binary; the
  verify script uses `git`, `sha256sum`/`shasum`, and `git verify-commit`
  (OpenSSH-backed) — all present on `ubuntu-latest`. Consistent with the repo's
  `cargo deny` / `cargo machete` discipline.

## Testing

- **Harness self-test:** ingest a synthetic CCDA fixture, call `list_problems`,
  assert the expected problem appears — proves the stdio/JSON-RPC plumbing.
- **Gate, lockfile:** edit a `holdout/` file without re-blessing; confirm
  `just holdout-verify` (and CI) fails naming the file. Delete a holdout file;
  confirm the lock mismatch fires.
- **Gate, signature:** push an unsigned commit touching `holdout/`; confirm CI
  fails. Push a signed (blessed) commit; confirm CI passes. Confirm a commit
  touching only `crates/**` needs no signature.
- **Bless loop:** run `just holdout-bless`, confirm the Touch ID prompt, confirm
  the resulting commit verifies against `.github/allowed_signers`.
- **End-to-end:** add a signed failing holdout test, let a fix make it pass,
  squash-merge, confirm the PR check stays green throughout and the merge is
  permitted only when the suite passes.

## Out of scope

- Separate-repo / external-CI isolation (the adversarial-agent threat model).
- Rust-public-API-level holdout tests (the churning surface; black-box only).
- Auto-generating holdout tests from bug reports.
- Signature preservation on `main` under squash-merge.
- Porting the bless key off the Secure Enclave (YubiKey / passphrase variants)
  — documented as alternatives but not built.
