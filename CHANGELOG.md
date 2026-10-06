# Changelog

All notable user-visible changes to DataJig are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.6.0] - 2026-10-06

### Added

- Optional `datajig[duckdb]` execution provider, exactly pinned to DuckDB 1.5.6,
  for bounded transforms over explicit CSV, flat Parquet, and JSONL aliases.
- `transform-plan`, `transform-apply`, and read-only `transform-info` workflows
  with deterministic `xform_...` plans, explicit authorization, drift checks,
  recoverable no-clobber publication, and `xformed_...` receipts.
- Rust-owned SQL AST authorization that permits one bounded SELECT and denies
  arbitrary file/table functions, undeclared relations, COPY, ATTACH, extension
  loading, network access, multiple statements, and volatile functions.
- Independent Rust verification of provider output schema, supported scalar
  types, canonical JSONL bytes, unique non-null IDs, limits, and predicted
  content identity.
- Revision schema 3 transform lineage binding the verified plan, provider,
  sources, query, parameters, receipt, and output into workspace history.
- Machine-readable transform command descriptors, artifact schemas,
  capabilities, resource limits, Agent Skill guidance, and actionable optional
  provider installation errors.

### Changed

- The Python entrypoint now binds its active interpreter only for native
  transform commands and strips inherited provider-executable overrides.
- `init` accepts `--source-receipt` and re-verifies the receipt, output bytes,
  and ID field before creating any workspace state.

## [0.5.1] - 2026-10-06

### Added

- `datajig tutorial OUTPUT` runs a complete keyed-JSONL change-control and
  verified training-export workflow in a new self-contained directory.
- Failed checks expose immediate bounded `plan` and `findings` actions.

### Changed

- `check`, `plan`, `status`, and `seal` automatically resolve a unique active
  change and staged candidate when both long identifiers are omitted.
- Python console-script help for native commands now preserves the authoritative
  Rust usage, examples, aliases, and argument descriptions.
- Training exports accept one-record and one-byte shard targets. The byte target
  is soft when one valid record must occupy an oversize singleton shard.
- Public format and platform documentation now distinguishes native CSV/Parquet
  preparation, keyed-JSONL workspaces, WSL support, and planned adapters.

## [0.5.0] - 2026-10-06

### Added

- Agent-oriented CLI remediation: invalid arguments now include structured,
  executable `next_actions`; split syntax and basis-point constraints are
  discoverable through help and capabilities; and `check` accepts deterministic
  `@active`/`@latest` selectors when exactly one compatible candidate exists.
- Repository hygiene enforcement in local commit hooks and CI, preventing
  private development directories, generated local state, and disallowed
  contributor identities from entering commits or reachable history.
- Repository-managed installation and verification through `repository-install`
  and read-only `repository-check`, including a content-addressed contract lock,
  version-matched Agent Skill, SHA-pinned GitHub Actions workflow, pre-commit
  hook, bound-workspace readiness checks, safe upgrades, and crash recovery.
- Proof-carrying training-consumption plans and receipts for Python, PyTorch,
  and Hugging Face adapters, including multi-worker shard markers, fail-closed
  local state, crash recovery, and an explicit at-least-once boundary claim.
- Import-receipt-aware CSV, Parquet, and JSONL preparation with deterministic
  multi-shard ordering, include/ignore selection, schema checks, and end-to-end
  Hugging Face import-to-workspace verification.
- Revision-pinned Hugging Face dataset-repository imports with explicit
  plan/accept/apply authorization, atomic publication, and verified receipts.
- Agent-scoped keyed JSONL workspaces with immutable revisions, deterministic
  identities, quality policies, and evidence-bound review and sealing.
- Guarded JSONL repair drafting, preview, atomic application, exact undo, and
  bounded finding lookup without exposing raw record identifiers or values.
- Deterministic CSV and flat Parquet preparation through identity-bound
  plan/apply workflows with provenance receipts and crash-recoverable output.
- Privacy-safe direct inspection for keyed JSONL, CSV, and flat Parquet data,
  including preparation recipe scaffolds for tabular inputs.
- Recoverable retained bytes for keyed JSONL revisions, including exact
  materialization and verified training-bundle export from historical revisions.
- Deterministic subset views and training exports with verified Python, PyTorch,
  and Hugging Face consumption.
- Installable Linux and macOS wheels containing the Rust core.
- Deterministic `agent_contract_id` discovery and generated-Skill pinning for
  command and artifact-schema compatibility checks.
- Rust contract tests for Agent discovery, artifact examples, snapshot
  identity, strict manifest parsing, and fail-closed error envelopes.

### Changed

- Hugging Face HTTPS now uses Rustls with platform certificate verification,
  removing the system OpenSSL build dependency; Unix metadata preservation is
  portable across Linux and macOS mode types.
- Training-bundle publication now reports an actionable filesystem diagnostic
  when atomic no-replace directory publication is unavailable. File publication
  uses a safe hard-link fallback while preserving no-clobber semantics.
- The command-line and artifact contracts expose bounded, machine-readable
  decisions and next actions for agent workflows.
