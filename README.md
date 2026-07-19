# ChartPDS

ChartPDS is a personal data store for clinical and wearable-device health data.
"PDS" stands for *Personal Data Store* in the Solid sense (a self-hosted,
single-user repository), not the Bluesky sense (a network-accessible server).
ChartPDS does not serve data over the network; it exposes its contents through
an MCP server consumed locally by agent harnesses.

This is a Rust rewrite of the [`vitals`](https://github.com/fhwang/vitals)
TypeScript project.

## What it does

- Ingests structured clinical documents (CCDA XML) and narrative clinical
  PDFs (pathology/imaging reports, visit notes). Narrative ingest runs a
  one-time, mechanically verified LLM extraction of explicitly quoted
  ICD-10-CM codes (requires an Anthropic API key).
- Syncs wearable data: Fitbit heart rate (via the Google Health API) and
  Oura sleep stages (via the Oura v2 API), with a background sync daemon.
- Archives every raw payload in a content-addressed blob store; the SQLite
  index is a disposable projection that can be rebuilt from the archive
  offline at any time.
- Answers analytical queries over the result: latest/history per coding,
  descriptive statistics, aligned multi-signal tables, two-signal
  relationships, full-text search over narrative documents, and current
  problem/medication lists.

## Using it from an agent harness

Build the server, then register it with your MCP-capable harness (Claude
Code, Claude Desktop, or anything else that speaks MCP over stdio):

```sh
cargo build --release   # produces target/release/chartpds-mcp
```

```json
{
  "mcpServers": {
    "chartpds": {
      "command": "/path/to/ChartPDS/target/release/chartpds-mcp",
      "env": {
        "CHARTPDS_DATA_DIR": "/path/to/chartpds-data",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

Tool names, descriptions, and argument schemas are served over the protocol
itself — any MCP client gets them from `tools/list`, so the listing below is
an orientation map, not the reference. To browse the live tool surface
without a harness:

```sh
npx @modelcontextprotocol/inspector \
    bash -c 'CHARTPDS_DATA_DIR=/path/to/chartpds-data cargo run --bin chartpds-mcp'
```

The tool surface, grouped:

- **Ingest & maintenance** — `record_ingest` (CCDA XML or narrative
  clinical PDF), `index_rebuild` (offline replay of the archive)
- **Observations & analytics** — `observation_codings`,
  `coding_definitions`, `observation_latest`, `observation_history`,
  `observation_stats`, `observation_table`, `observation_relationship`
- **Clinical record** — `problem_list`, `medication_list`,
  `narrative_search`, `narrative_get`
- **Sources & operations** — `source_connect`, `source_sync`,
  `notification_list`

## Configuration

All configuration is by environment variable (see `.env.example`):

| Variable | Required | Purpose |
| --- | --- | --- |
| `CHARTPDS_DATA_DIR` | yes | Data root: SQLite index, `archive/`, `derived/` (created if absent) |
| `ANTHROPIC_API_KEY` | for narrative-PDF ingest | One-time LLM extraction during `record_ingest` of `kind="clinical-pdf"`; no other tool needs it |
| `ANTHROPIC_BASE_URL` | no | Anthropic endpoint override (proxies/gateways) |
| `GOOGLE_HEALTH_CLIENT_ID`, `GOOGLE_HEALTH_CLIENT_SECRET` | for Fitbit | OAuth client for the Google Health API |
| `OURA_PERSONAL_ACCESS_TOKEN` | for Oura | Oura v2 personal access token |
| `CHARTPDS_SYNC_INTERVAL_SECS` | no | Background sync interval, seconds (default 300; `0` disables the daemon) |

Adapter setup walkthroughs (cloud console steps, token generation) are in
the module docs: `crates/chartpds-core/src/sources/fitbit/mod.rs` and
`crates/chartpds-core/src/sources/oura/mod.rs`.

## Development

- Rust stable (version pinned in `rust-toolchain.toml`).
- [`just`](https://github.com/casey/just) for task orchestration.

```sh
just install-tools   # one-time: installs sqlx-cli, cargo-deny, cargo-machete
just check           # fmt-check, lint, typecheck, test, deny, machete
```

To run the server directly (it reads MCP JSON-RPC frames on stdin, writes
responses to stdout, and logs to stderr; press Ctrl-D to shut down):

```sh
export CHARTPDS_DATA_DIR='/path/to/chartpds-data'
cargo run --bin chartpds-mcp
```

Contributor docs live in `CLAUDE.md` (policy, workflow, invariants — with
pointers into the rustdoc, where the reference documentation lives).

## Layout

```
crates/
├── chartpds-core/   # library: archive, ingestion, clinical, index,
│                    #          queries, sources, sync, notifications
└── chartpds-mcp/    # binary: MCP stdio server
holdout/             # tamper-resistant black-box regression suite
```
