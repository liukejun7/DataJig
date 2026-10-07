# Changelog

All notable user-visible changes to DataJig are documented in this file. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.8.3] - 2026-10-07

### Added

- `artifact-schema pipeline-config` with a strict complete schema and an
  executable canonical example; `pipeline plan --help` now includes the minimal
  legal shape and relative-path rules.
- A CI journey that generates three days of raw logs and executes directory
  preparation, feature aggregation, lineage-bound versioning, review/seal,
  training export, PyTorch consumption planning, pipeline delivery, and lineage
  verification.

### Changed

- Unknown pipeline fields report every allowed field and return direct
  `artifact-schema pipeline-config` and `pipeline plan --help` actions.
- README and package documentation state the current pipeline-only lineage
  lookup boundary and the deterministic-transform approach for source data
  without a stable key.

### Fixed

- DuckDB conversion and query failures now preserve a bounded actionable
  diagnostic, including the offending value, column, and target type when
  available, instead of appearing as an unexplained provider crash. SQL text
  and unbounded engine output remain suppressed, and failed transforms publish
  no plan or dataset.

## [0.8.2] - 2026-10-06

### Added

- `transform-plan --params <JSON_ARRAY>` for direct scalar parameter binding,
  plus explicit `--params-file <PATH>` input for reusable parameter documents.
- Machine-readable transform limit details with metric, observed value,
  lower-bound semantics, configured limit, and unit.
- Parallel real-data CI shards, Python 3.11–3.13 workflow compatibility tests,
  and installed-wheel tutorial verification on every release platform.
- Idempotent GitHub Release creation with automatic attachment of all four
  verified platform wheels.

### Fixed

- Inline transform parameters are no longer interpreted as filesystem paths,
  avoiding misleading missing-file and file-name-too-long failures.
- Pipeline transforms now use the explicit parameter-file contract.

## [0.8.1] - 2026-10-06

### Added

- Safe fixed-path delivery evolution for eligible update pipelines using native
  atomic directory exchange, target-filesystem preflight, retained verified
  backups, and strictly forward crash recovery after HEAD advances.
- Explicit capability fields for artifact-file shape, CLI response envelope,
  and atomic delivery-update support.

### Changed

- Preparation help now includes a minimal recipe and points directly to the
  complete `artifact-schema prepare-recipe` contract.
- Transform row, byte, field, and wall-time failures report the observed value
  (or a bounded lower estimate) together with the configured limit.
- Agent-facing error responses consistently include a `next_actions` array.

### Fixed

- DuckDB CSV transforms accept mixed CRLF/LF record endings without rewriting
  source bytes, including quoted fields with embedded newlines.
- CSV load failures retain real parser line numbers when available, identify
  the source alias, and never invent a location when DuckDB provides none.
- Preparation recipe identity failures now identify the invalid namespace,
  kind, schema version, or output format instead of returning one opaque error.
- README badges no longer depend on private GitHub repository metadata.

## [0.8.0] - 2026-10-06

### Added

- `pipeline plan/apply/info/gc` for one-config local training-data deliveries
  over CSV, Parquet, or JSONL inputs and the bounded DuckDB provider.
- Strict YAML normalization with duplicate-key, tag, anchor, alias, timestamp,
  unknown-field, non-finite scalar, and symbolic-link rejection.
- Deterministic relocation-stable `pipe_...` authorization and bundle-spec
  identities, with deployment-bound `piped_...` success receipts.
- Durable execution journals, per-plan and per-workspace OS advisory locks,
  explicit `--resume`, create/update/no-op behavior, and commit-marker delivery
  visibility.
- Detached update sealing, historical bundle preparation, and compare-and-swap
  HEAD commit so a stale pipeline cannot advance a workspace.
- `lineage` output across source, transform, revision, bundle, and consumption
  plan, plus recent-pipeline context in `status`.

### Changed

- The recommended path from local raw data to training is now a reviewed
  Pipeline; all 0.7 atomic commands remain available and compatible.
- `capabilities` advertises pipeline plan, receipt, and lineage schema version 1.

### Fixed

- Interrupted delivery publication can recover after HEAD CAS, directory
  rename, or commit-marker publication without silently overwriting output.
- Pipeline plans now reject both authorization-field tampering and changes
  hidden behind a forged bundle-spec identity.

## [0.7.0] - 2026-10-06

### Added

- Recursive local CSV, flat Parquet, and JSONL dataset preparation with bounded
  include/ignore selectors, canonical shard ordering, a deterministic
  `source_set_...` identity, and membership-drift rejection at apply time.
- Machine-readable `requires` prerequisites in every command descriptor.
- Workspace status now identifies the tracked dataset path and adapter.

### Changed

- Training split weights are positive relative integers such as `7/2/1`; the
  core deterministically normalizes them to 10,000 allocation units.
- `transform-plan --sql` now means inline SQL. File input uses the explicit
  `--sql-file` option, and immutable schema-2 plans bind SQL content without a
  live SQL-file dependency during apply.
- The remediation command is now named `review-plan`, avoiding ambiguity with
  preparation, import, transform, and consumption plans.
- Top-level help presents the complete lifecycle, while split and transform
  help expose their non-obvious argument contracts before execution.
- The project description and repository header now use the concise
  agent-native data-control-plane positioning; platform metadata no longer
  claims unsupported native Windows behavior.

### Fixed

- Status output no longer forces agents to rediscover which dataset a state
  directory controls.
- SQL file paths are no longer part of transform identity or a source of false
  apply-time drift after their contents have been authorized.
- Relative training splits no longer require callers to manually encode basis
  points summing to exactly 10,000.

## [0.6.1] - 2026-10-06

### Added

- Top-level `datajig --version` output sourced from installed package metadata.

### Changed

- GitHub Actions and generated repository workflows now use immutable action
  revisions backed by the Node 24 runtime.
- The GitHub overview now uses a repository-relative, friendly illustrated hero
  and leads with DataJig's agent-native control-plane positioning. PyPI uses a
  dedicated English description without repository-relative media.

### Fixed

- The README hero no longer depends on unauthenticated raw access to a private
  repository, which previously rendered as a broken image.

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
