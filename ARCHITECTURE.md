# DataJig Architecture

DataJig is an agent-native control plane for training data. It sits between raw
sources and model-training consumers and makes each material change explicit,
reviewable, reproducible, and addressable by identity.

```text
local files / Hugging Face dataset repositories
                    │
                    ▼
       source adapters and immutable plans
                    │
                    ▼
        inspection and deterministic prepare
                    │
                    ▼
       task workspace → preview → apply → check
                    │
                    ▼
           sealed revisions and subset views
                    │
                    ▼
       verified training bundles and consumers
```

## Trust model

DataJig uses a plan/accept/apply protocol for operations that create or modify
data. Planning resolves inputs, predicts effects, writes a bounded artifact, and
returns its deterministic identity. Apply requires that exact identity and
revalidates the live inputs before publishing anything. An agent cannot silently
substitute a moving branch, stale evidence, or a different output.

Identities form a provenance chain rather than a mutable label. Examples include
`hfplan_...` and `hfimport_...` for source acquisition, `prep_...` for tabular
preparation, `changeset_...` and `review_...` inside a task workspace, `rev_...`
for accepted data, `view_...` for a deterministic subset, and `bundle_...` for a
training handoff.

Important failure boundaries are fail-closed:

- moving Hugging Face revisions are resolved to a 40-character commit before
  files are selected;
- paths are canonicalized and checked against traversal and symbolic-link
  escapes;
- review evidence is rejected when the underlying bytes have changed;
- new outputs are staged next to their destination, verified, synced, and
  atomically published without replacing an existing path;
- agent-facing JSON, pagination, file counts, byte counts, and path lengths are
  bounded;
- authentication secrets are read through standard Hugging Face configuration
  and removed from surfaced errors.

## Component boundaries

The Rust core is authoritative for artifact parsing, canonical identity,
validation, native filesystem transactions, inspection, preparation, workspace
state, review, sealing, and export. The Python package distributes the native
binary, exposes the human-facing CLI, and supplies verified adapters for Python,
PyTorch, and Hugging Face Datasets consumers. It is not an algorithm fallback.

DataJig owns the behavior that must remain stable across engines:

- versioned contracts and deterministic identities;
- limits, validation, and privacy-safe summaries;
- authorization and transaction semantics;
- provenance, receipts, and integrity verification;
- sealed revisions, views, and training bundles.

It deliberately integrates existing engines where replacement would add no
trust value: Hugging Face transports source bytes, Parquet libraries decode
columnar data, and PyTorch or Hugging Face Datasets consumes verified bundles.
DataJig is not a replacement for Git, Oxen, DVC, object storage, or a training
orchestrator. Those systems move, retain, or train on data; DataJig controls and
proves the transitions between them.

## Source adapters

A source adapter must resolve an immutable source identity, produce a complete
bounded file manifest, and fetch exactly that manifest. The first remote adapter
supports Hugging Face dataset repositories. It resolves a branch, tag, or commit
once, selects repository files by glob, and verifies the downloaded set and local
BLAKE3 hashes before writing `datajig.hf-import.json`.

Future adapters for object stores and data-versioning systems should reuse the
same contract instead of leaking provider-specific mutable state into downstream
preparation or training.

## Platform boundary

Read-only inspection and integrations are portable. Native workspace writes,
atomic preparation, and Hugging Face publication are currently supported on
Linux and macOS, where the filesystem primitives required by the transaction
model are available. Windows write support remains explicit future work.

See [ROADMAP.md](ROADMAP.md) for the capability sequence and product boundary.
