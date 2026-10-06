# DataJig Roadmap

DataJig's target is the complete, verifiable path from raw data to model-training
input. The roadmap prioritizes workflows where an agent needs to act autonomously
without weakening review, reproducibility, or recovery.

## Shipped

- privacy-safe inspection for keyed JSONL, CSV, and flat Parquet;
- deterministic CSV/Parquet/JSONL-to-JSONL preparation with plan/apply receipts,
  including ordered multi-file inputs bound to verified Hugging Face imports;
- revision-pinned Hugging Face dataset-repository imports with exact file-set,
  size, and local content verification;
- task-scoped JSONL workspaces, quality policies, record-level diffs, guarded
  repair, review, atomic apply, and exact undo;
- sealed revisions, deterministic subset views, and verified training bundles;
- Python, PyTorch, and Hugging Face Datasets consumers that verify bundle
  identities and bytes before iteration;
- proof-carrying `consume_...` plans and `consumed_...` receipts that bind a
  public training run to one fully exhausted verified split;
- bounded Agent discovery, command schemas, next actions, and generated Skills.
- one-command repository installation for the version-matched Agent Skill,
  content-addressed lock, Git hook, and SHA-pinned CI verification, with
  fail-closed drift and clean-workspace checks.
- bounded DuckDB transforms over explicit CSV, Parquet, and JSONL aliases, with
  Rust AST authorization, plan/apply consent, canonical JSONL verification,
  provenance receipts, and revision-bound lineage.

## Near term

1. Stronger policy templates for leakage, licensing, schema drift, and
   training-readiness gates.
2. Arrow batch interchange, richer SQL type coverage, and multi-record patch
   sets while preserving the shipped transform plan/apply boundary.
3. Experiment-system receipt attachment and multi-host consumption evidence.

## Source and version adapters

- S3, Google Cloud Storage, and Azure Blob immutable-object manifests;
- Oxen and DVC revision adapters;
- Git/LFS sources where their object identity can be made explicit;
- additional dataset hubs through the same immutable adapter contract.

Adapters will resolve and verify source state. They will not turn mutable remote
locations into implicit inputs or make provider credentials part of artifacts.

## Later

- distributed execution behind the same deterministic artifact contracts;
- catalog and lineage views over local and remote revisions;
- organization policy, attestations, and approval integration;
- richer multimodal review and dataset-wide validation.

## Explicitly outside the product boundary

- full model-training orchestration, hyperparameter scheduling, and model serving;
- replacing Git, Oxen, DVC, object stores, or dataset hubs as storage systems;
- arbitrary remote-code execution during data import;
- treating Hugging Face Dataset Viewer rows, configurations, or generated splits
  as repository files in the first adapter version.

Work is prioritized when it closes a gap between raw source and verified training
input, compounds DataJig's identity/provenance model, and is safe for an agent to
drive without hidden state.
