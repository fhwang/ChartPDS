# ChartPDS

ChartPDS is a personal data store for clinical and wearable-device health data.
"PDS" stands for *Personal Data Store* in the Solid sense (a self-hosted,
single-user repository), not the Bluesky sense (a network-accessible server).
ChartPDS does not serve data over the network; it exposes its contents through
an MCP server consumed locally by agent harnesses.

This is a Rust rewrite of the [`vitals`](https://github.com/fhwang/vitals)
TypeScript project.

## Status

Active development. Phases 0-15 implemented: workspace scaffold, content-
addressed archive, clinical taxonomies, SQLite index, CCDA ingestion
pipeline, query primitives, MCP server with 10 tools, source infrastructure
tables, Fitbit heart-rate adapter (via Google Health API), Oura sleep-stage
adapter (via Oura v2 API), background sync daemon with Source trait
abstraction, rebuild-index, and notification system.

## Requirements

- Rust stable (version pinned in `rust-toolchain.toml`).
- [`just`](https://github.com/casey/just) for task orchestration.

## Setup

```sh
just install-tools   # one-time: installs cargo-deny and cargo-machete
just check           # runs fmt-check, lint, typecheck, test, deny, machete
```

## Running the MCP server

```sh
export CHARTPDS_DATA_DIR='/path/to/chartpds-data'
cargo run --bin chartpds-mcp
```

The binary reads MCP JSON-RPC frames from stdin and writes responses
to stdout. Logs go to stderr. Press Ctrl-D to shut down. See
`.env.example` for all configuration variables.

To drive it interactively with the MCP inspector:

```sh
npx @modelcontextprotocol/inspector \
    bash -c 'CHARTPDS_DATA_DIR=/path/to/chartpds-data cargo run --bin chartpds-mcp'
```

## Layout

```
crates/
├── chartpds-core/   # library: archive, ingestion, clinical, index,
│                    #          queries, sources, sync, notifications
└── chartpds-mcp/    # binary: MCP stdio server (10 tools)
```
