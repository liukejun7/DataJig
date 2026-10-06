use crate::io::save_file_atomically;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::json;
use std::path::Path;

pub const AGENT_API_VERSION: u8 = 1;
pub const AGENT_SKILL_SCHEMA_VERSION: u8 = 1;
pub const COMMAND_SCHEMA_VERSION: u8 = 1;
const MAX_AGENT_SKILL_BYTES: usize = 64 * 1024;

pub fn agent_contract_id() -> String {
    let artifact_schemas = crate::artifact_schema::artifact_schema_names()
        .into_iter()
        .map(|name| {
            crate::artifact_schema::artifact_schema(name)
                .expect("cataloged artifact schemas must be available")
        })
        .collect::<Vec<_>>();
    let contract = json!({
        "agent_api_version": AGENT_API_VERSION,
        "command_schema_version": COMMAND_SCHEMA_VERSION,
        "artifact_schema_version": crate::artifact_schema::ARTIFACT_SCHEMA_VERSION,
        "commands": command_catalog(),
        "artifact_schemas": artifact_schemas,
    });
    let payload =
        serde_json::to_vec(&contract).expect("the static agent contract must serialize as JSON");
    crate::identity::blake3_content_id("contract", b"datajig-agent-contract-v1\0", &payload)
}

#[derive(Clone, Debug, Serialize)]
pub struct CommandDescriptor {
    pub name: &'static str,
    summary: &'static str,
    usage: &'static str,
    read_only: bool,
    effects: Vec<&'static str>,
    platforms: Vec<&'static str>,
    inputs: Vec<InputDescriptor>,
    output: OutputDescriptor,
    exit_codes: Vec<ExitCodeDescriptor>,
}

#[derive(Clone, Debug, Serialize)]
struct InputDescriptor {
    name: &'static str,
    kind: &'static str,
    required: bool,
    repeatable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<&'static str>,
    description: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct OutputDescriptor {
    kind: &'static str,
    channel: &'static str,
    bounded: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ExitCodeDescriptor {
    code: u8,
    meaning: &'static str,
}

struct AccessDescriptor {
    read_only: bool,
    effects: Vec<&'static str>,
    platforms: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AgentSkillArtifact {
    kind: &'static str,
    schema_version: u8,
    content_id: String,
    bytes: usize,
    output: String,
}

pub fn command_catalog() -> Vec<CommandDescriptor> {
    let commands = vec![
        command(
            "agent-skill",
            "Generate a version-matched Agent Skill for the installed CLI.",
            "datajig agent-skill --output <PATH>",
            writes(vec!["write_agent_configuration"], unix_platforms()),
            vec![input(
                "output",
                "path",
                true,
                false,
                None,
                "Destination SKILL.md path.",
            )],
            output("agent_skill_created", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "artifact-schema",
            "Return a machine-readable JSON Schema and canonical example for a DataJig input artifact.",
            "datajig artifact-schema [jsonl-field-patch|prepare-recipe|repository-integration|subset-view|training-consumption-plan|training-consumption-receipt]",
            read_only(vec![], all_platforms()),
            vec![input(
                "artifact",
                "enum",
                false,
                false,
                None,
                "Optional artifact name; omit it to list supported schema names.",
            )],
            output("artifact_schema", "stdout", true),
            vec![
                exit(0, "schema catalog or requested schema returned"),
                exit(4, "artifact schema not found"),
            ],
        ),
        command(
            "capabilities",
            "Describe backend availability, versions, features, and limits.",
            "datajig capabilities",
            read_only(vec![], all_platforms()),
            vec![],
            output("capabilities", "stdout", true),
            success_only(),
        ),
        command(
            "changeset-begin",
            "Declare an Agent data task against the current clean dataset HEAD.",
            "datajig changeset-begin --intent <TEXT> --task-id <ID> [--state <DIR>] [--threads <1..8>] [--actor-kind <KIND>]",
            writes(
                vec!["read_dataset", "read_workspace", "write_changeset_object"],
                unix_platforms(),
            ),
            vec![
                input(
                    "intent",
                    "string",
                    true,
                    false,
                    None,
                    "Bounded human-readable purpose of the data task.",
                ),
                input(
                    "task_id",
                    "string",
                    true,
                    false,
                    None,
                    "External Agent or research task identifier.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Schema-2 workspace state directory.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Worker count; valid range is 1 through 8.",
                ),
                input(
                    "actor_kind",
                    "string",
                    false,
                    false,
                    Some("agent"),
                    "Actor category recorded with the declaration.",
                ),
            ],
            output("change_declared", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "changeset-stage",
            "Capture the current full dataset as an immutable staged changeset.",
            "datajig changeset-stage --change <chg_ID> [--state <DIR>] [--threads <1..8>]",
            writes(
                vec!["read_dataset", "read_workspace", "write_changeset_object"],
                unix_platforms(),
            ),
            vec![
                input(
                    "change",
                    "content_id:chg",
                    true,
                    false,
                    None,
                    "Change declaration returned by changeset-begin.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Schema-2 workspace state directory.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Worker count; valid range is 1 through 8.",
                ),
            ],
            output("changeset_staged", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "check",
            "Review the tracked dataset against its last-good baseline and return an explicit decision.",
            "datajig check [--state <DIR>] [--threads <1..8>] [--phash-threshold <0..64>] [--change <chg_ID|@active|@latest> --changeset <changeset_ID|@latest|@active>]",
            writes(
                vec!["read_dataset", "read_workspace", "write_review"],
                unix_platforms(),
            ),
            vec![
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory created by init.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Worker count; valid range is 1 through 8.",
                ),
                input(
                    "phash_threshold",
                    "integer",
                    false,
                    false,
                    Some("6"),
                    "Maximum 64-bit pHash Hamming distance; valid range is 0 through 64.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    false,
                    false,
                    None,
                    "Optional task declaration; @active and @latest resolve only when one compatible declaration exists; omit both selectors to resolve the unique active pair; requires changeset when supplied.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    false,
                    false,
                    None,
                    "Optional staged candidate; @latest and @active resolve only when one compatible stage exists; omit both selectors to resolve the unique active pair; requires change when supplied.",
                ),
            ],
            output("workspace_checked", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "consume-info",
            "Verify one accepted consumption plan and its exact live training bundle split.",
            "datajig consume-info <PLAN> --verify --accept-plan <consume_ID>",
            read_only(
                vec!["read_consumption_plan", "read_training_bundle"],
                unix_platforms(),
            ),
            vec![
                input(
                    "plan",
                    "path",
                    true,
                    false,
                    None,
                    "Schema-1 consumption plan.",
                ),
                input(
                    "verify",
                    "boolean_flag",
                    true,
                    false,
                    None,
                    "Reverify the manifest and every selected shard.",
                ),
                input(
                    "accept_plan",
                    "content_id:consume",
                    true,
                    false,
                    None,
                    "Exact authorized consumption plan identity.",
                ),
            ],
            output("training_consumption_info", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "consume-plan",
            "Plan one named adapter-boundary consumption of an exact verified bundle split.",
            "datajig consume-plan <MANIFEST> --split <NAME> --consumer <python|pytorch|huggingface> --run-id <ID> --output <NEW_DIR> --plan <NEW_JSON>",
            writes(
                vec!["read_training_bundle", "write_consumption_plan"],
                unix_platforms(),
            ),
            vec![
                input(
                    "manifest",
                    "path",
                    true,
                    false,
                    None,
                    "Training bundle datajig.bundle.json.",
                ),
                input(
                    "split",
                    "string",
                    true,
                    false,
                    None,
                    "One non-empty verified bundle split.",
                ),
                input(
                    "consumer",
                    "enum:python|pytorch|huggingface",
                    true,
                    false,
                    None,
                    "Adapter boundary that will publish completion markers.",
                ),
                input(
                    "run_id",
                    "string",
                    true,
                    false,
                    None,
                    "Public external run identity; 1 through 256 non-control UTF-8 bytes.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "New local run-state directory.",
                ),
                input(
                    "plan",
                    "path",
                    true,
                    false,
                    None,
                    "New schema-1 consumption plan artifact.",
                ),
            ],
            output("training_consumption_planned", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "describe",
            "Return the versioned machine contract for all commands or one command.",
            "datajig describe [COMMAND]",
            read_only(vec![], all_platforms()),
            vec![input(
                "command",
                "string",
                false,
                false,
                None,
                "Optional command name to describe.",
            )],
            output("command_catalog_or_descriptor", "stdout", true),
            vec![exit(0, "success"), exit(4, "command not found")],
        ),
        command(
            "explain",
            "Produce a bounded review summary for an agent context window.",
            "datajig explain <REPORT> [--limit <0..20>]",
            read_only(vec!["read_artifact"], all_platforms()),
            vec![
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Review report JSON file.",
                ),
                input(
                    "limit",
                    "integer",
                    false,
                    false,
                    Some("10"),
                    "Maximum findings to include; valid range is 0 through 20.",
                ),
            ],
            output("review_explanation", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "export",
            "Export clean sealed JSONL HEAD or any reachable immutable revision as deterministic, verifiable training shards.",
            "datajig export --output <NEW_DIR> --split <NAME=WEIGHT>... [--revision <rev_ID>] [--view <RECIPE>] [--state <DIR>] [--seed <TEXT>] [--max-shard-records <N>] [--max-shard-bytes <N>]",
            writes(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_revision",
                    "read_blob",
                    "read_subset_recipe",
                    "write_training_bundle",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "New directory for the portable training bundle; existing paths are rejected.",
                ),
                input(
                    "split",
                    "name=integer_weight",
                    true,
                    true,
                    None,
                    "Deterministic split weight; all weights must total exactly 10000.",
                ),
                input(
                    "revision",
                    "content_id:rev",
                    false,
                    false,
                    Some("clean live HEAD"),
                    "Reachable immutable JSONL revision to export; explicit revisions use their verified retained content.",
                ),
                input(
                    "view",
                    "path",
                    false,
                    false,
                    None,
                    "Optional schema-1 subset recipe; the selected view is embedded and verified in the bundle.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Clean sealed keyed-JSONL workspace state directory.",
                ),
                input(
                    "seed",
                    "string",
                    false,
                    false,
                    Some("datajig-v1"),
                    "Stable split and ordering seed; 1 through 256 UTF-8 bytes.",
                ),
                input(
                    "max_shard_records",
                    "integer",
                    false,
                    false,
                    Some("10000"),
                    "Record target per shard; valid range is 1 through 250000; final shards may contain fewer records.",
                ),
                input(
                    "max_shard_bytes",
                    "integer",
                    false,
                    false,
                    Some("268435456"),
                    "Soft byte target per shard; valid range is 1 through 1073741824; a larger single record occupies its own shard.",
                ),
            ],
            output("training_bundle_created", "stdout", true),
            vec![
                exit(0, "training bundle created"),
                exit(
                    2,
                    "invalid input, corrupt content, contention, or I/O failure",
                ),
                exit(4, "retained revision content is unavailable"),
            ],
        ),
        command(
            "export-info",
            "Read, verify, and optionally pin a portable training bundle for framework consumption.",
            "datajig export-info <MANIFEST> [--verify [--consumer-plan] [--expect-bundle <bundle_ID>] [--expect-revision <rev_ID>] [--require-assurance <LEVEL>] [--split <NAME>]]",
            read_only(vec!["read_training_bundle"], all_platforms()),
            vec![
                input(
                    "manifest",
                    "path",
                    true,
                    false,
                    None,
                    "Training bundle datajig.bundle.json manifest.",
                ),
                input(
                    "verify",
                    "boolean_flag",
                    false,
                    false,
                    Some("false"),
                    "Recompute every shard count, byte size, and content identity.",
                ),
                input(
                    "consumer_plan",
                    "boolean_flag",
                    false,
                    false,
                    Some("false"),
                    "Return a bounded schema-1 plan containing only verified relative shard descriptors.",
                ),
                input(
                    "expect_bundle",
                    "content_id:bundle",
                    false,
                    false,
                    None,
                    "Require the exact trusted bundle identity.",
                ),
                input(
                    "expect_revision",
                    "content_id:rev",
                    false,
                    false,
                    None,
                    "Require the exact sealed source revision.",
                ),
                input(
                    "require_assurance",
                    "enum:structural|quality_policy",
                    false,
                    false,
                    None,
                    "Reject a bundle below this assurance level.",
                ),
                input(
                    "split",
                    "string",
                    false,
                    false,
                    None,
                    "Return only this split in the consumer plan.",
                ),
            ],
            output("training_bundle_info", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "finding",
            "Get one review finding by its stable identifier.",
            "datajig finding <REPORT> <FINDING_ID>",
            read_only(vec!["read_artifact"], all_platforms()),
            vec![
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Review report JSON file.",
                ),
                input(
                    "finding_id",
                    "string",
                    true,
                    false,
                    None,
                    "Stable finding identifier returned by findings.",
                ),
            ],
            output("finding", "stdout", true),
            vec![
                exit(0, "success"),
                exit(2, "invalid argument or report"),
                exit(4, "finding not found"),
            ],
        ),
        command(
            "findings",
            "List and filter review findings with bounded pagination.",
            "datajig findings <REPORT> [--severity <LEVEL>]... [--code <CODE>]... [--offset <N>] [--limit <1..200>]",
            read_only(vec!["read_artifact"], all_platforms()),
            vec![
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Review report JSON file.",
                ),
                input(
                    "severity",
                    "enum:error|warning|info",
                    false,
                    true,
                    None,
                    "Severity filter; may be repeated.",
                ),
                input(
                    "code",
                    "string",
                    false,
                    true,
                    None,
                    "Finding-code filter; may be repeated.",
                ),
                input(
                    "offset",
                    "integer",
                    false,
                    false,
                    Some("0"),
                    "Zero-based finding offset.",
                ),
                input(
                    "limit",
                    "integer",
                    false,
                    false,
                    Some("50"),
                    "Maximum findings to return; valid range is 1 through 200.",
                ),
            ],
            output("finding_page", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "hf-import-apply",
            "Download, verify, and atomically publish one accepted immutable Hugging Face dataset import.",
            "datajig hf-import-apply <PLAN> --accept-plan <hfplan_ID>",
            writes(
                vec![
                    "read_hf_import_plan",
                    "read_hugging_face_dataset",
                    "write_dataset",
                    "write_hf_import_receipt",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "plan",
                    "path",
                    true,
                    false,
                    None,
                    "Bounded plan artifact created by hf-import-plan.",
                ),
                input(
                    "accept_plan",
                    "content_id:hfplan",
                    true,
                    false,
                    None,
                    "Exact plan identity returned by hf-import-plan; explicit authorization boundary.",
                ),
            ],
            output("hugging_face_import_applied", "stdout", true),
            vec![
                exit(
                    0,
                    "verified dataset and import receipt published or already present",
                ),
                exit(
                    2,
                    "plan not authorized, download invalid, or destination unsafe",
                ),
            ],
        ),
        command(
            "hf-import-plan",
            "Resolve one Hugging Face dataset revision to an immutable commit and plan selected files.",
            "datajig hf-import-plan <OWNER/DATASET> [--revision <REV>] [--include <GLOB>]... [--ignore <GLOB>]... --output <DIR> --plan <PLAN.json>",
            writes(
                vec!["read_hugging_face_dataset_metadata", "write_hf_import_plan"],
                unix_platforms(),
            ),
            vec![
                input(
                    "repository",
                    "hugging_face_dataset",
                    true,
                    false,
                    None,
                    "Dataset repository ID in owner/name form.",
                ),
                input(
                    "revision",
                    "string",
                    false,
                    false,
                    Some("main"),
                    "Branch, tag, or commit resolved once to an immutable commit SHA.",
                ),
                input(
                    "include",
                    "glob",
                    false,
                    true,
                    None,
                    "Selection glob; when omitted, supported data-file extensions are selected.",
                ),
                input(
                    "ignore",
                    "glob",
                    false,
                    true,
                    None,
                    "Exclusion glob applied after includes.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "New dataset directory bound into the plan.",
                ),
                input(
                    "plan",
                    "path",
                    true,
                    false,
                    None,
                    "New file for the content-bound import plan.",
                ),
            ],
            output("hugging_face_import_planned", "stdout", true),
            vec![
                exit(0, "immutable commit and selected file plan created"),
                exit(
                    2,
                    "repository, revision, filters, limits, or paths are invalid",
                ),
            ],
        ),
        command(
            "init",
            "Track a local dataset and record its initial last-good baseline.",
            "datajig init <DATASET> [--id-field <FIELD>] [--policy <PATH>] [--state <DIR>] [--threads <1..8>]",
            writes(vec!["read_dataset", "write_workspace"], unix_platforms()),
            vec![
                input(
                    "dataset",
                    "path",
                    true,
                    false,
                    None,
                    "ImageFolder directory or JSONL file to track.",
                ),
                input(
                    "id_field",
                    "string",
                    false,
                    false,
                    None,
                    "Required stable record key when DATASET is a JSONL file.",
                ),
                input(
                    "policy",
                    "path",
                    false,
                    false,
                    None,
                    "Optional schema-1 JSONL quality policy; creates a schema-4 workspace and supports changed_only or full gating.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory; it must stay outside the dataset.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Worker count; valid range is 1 through 8.",
                ),
            ],
            output("workspace_initialized", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "inspect",
            "Inspect JSONL, CSV, or flat Parquet structure and record identity without exposing values.",
            "datajig inspect <SOURCE.jsonl|SOURCE.csv|SOURCE.parquet> [--id-field <FIELD>] [--delimiter <ASCII>]",
            read_only(vec!["read_dataset"], all_platforms()),
            vec![
                input(
                    "source",
                    "path",
                    true,
                    false,
                    None,
                    "JSONL, headered CSV, or flat Parquet file to inspect.",
                ),
                input(
                    "id_field",
                    "string",
                    false,
                    false,
                    Some("id"),
                    "Top-level string or number field used as record identity.",
                ),
                input(
                    "delimiter",
                    "string",
                    false,
                    false,
                    Some(","),
                    "One-byte ASCII CSV delimiter; invalid for JSONL and Parquet.",
                ),
            ],
            output("dataset_inspection", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "inventory",
            "Inspect an ImageFolder dataset and write a media inventory.",
            "datajig inventory <ROOT> --output <OUTPUT> [--threads <1..8>]",
            writes(vec!["read_dataset", "write_artifact"], unix_platforms()),
            vec![
                input(
                    "root",
                    "path",
                    true,
                    false,
                    None,
                    "Dataset directory to inspect.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "Destination for the complete inventory JSON artifact.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Worker count; valid range is 1 through 8.",
                ),
            ],
            output("inventory_created", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "locate",
            "Resolve one verified staged JSONL finding to bounded current candidate line coordinates.",
            "datajig locate <REPORT> <FINDING_ID> --change <chg_ID> --changeset <changeset_ID> [--state <DIR>] [--offset <N>] [--limit <1..200>]",
            read_only(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_changeset_object",
                    "read_review",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Deterministic staged JSONL review returned by check.",
                ),
                input(
                    "finding_id",
                    "string",
                    true,
                    false,
                    None,
                    "Stable finding identifier returned by findings.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    true,
                    false,
                    None,
                    "Task declaration bound to the review.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    true,
                    false,
                    None,
                    "Staged JSONL candidate bound to the review.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory containing the immutable candidate state.",
                ),
                input(
                    "offset",
                    "integer",
                    false,
                    false,
                    Some("0"),
                    "Zero-based evidence-location offset.",
                ),
                input(
                    "limit",
                    "integer",
                    false,
                    false,
                    Some("50"),
                    "Maximum locations to return; valid range is 1 through 200.",
                ),
            ],
            output("finding_locations", "stdout", true),
            vec![
                exit(0, "success"),
                exit(2, "invalid, stale, or unreadable staged evidence"),
                exit(4, "finding not found"),
            ],
        ),
        command(
            "log",
            "Read bounded immutable dataset revision history, newest first.",
            "datajig log [--state <DIR>] [--offset <N>] [--limit <1..200>]",
            read_only(vec!["read_workspace"], unix_platforms()),
            vec![
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory created by init.",
                ),
                input(
                    "offset",
                    "integer",
                    false,
                    false,
                    Some("0"),
                    "Zero-based revision offset.",
                ),
                input(
                    "limit",
                    "integer",
                    false,
                    false,
                    Some("50"),
                    "Maximum revisions to return; valid range is 1 through 200.",
                ),
            ],
            output("revision_page", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "materialize",
            "Write the exact verified bytes of one reachable immutable keyed-JSONL revision.",
            "datajig materialize <REV> --output <PATH> [--state <DIR>]",
            writes(
                vec![
                    "read_workspace",
                    "read_revision",
                    "read_blob",
                    "write_artifact",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "revision",
                    "content_id:rev",
                    true,
                    false,
                    None,
                    "Exact revision identity reachable from workspace HEAD.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "New JSONL output outside the workspace state directory.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory created by init.",
                ),
            ],
            output("revision_materialized", "stdout", true),
            vec![
                exit(0, "revision materialized or exact output already present"),
                exit(
                    2,
                    "invalid, conflicting, corrupt, busy, or unreadable workspace",
                ),
                exit(4, "stored revision content unavailable"),
            ],
        ),
        command(
            "patch-apply",
            "Atomically apply one exact JSONL field repair accepted from patch-preview and prepare its replacement changeset.",
            "datajig patch-apply <REQUEST> <REPORT> --change <chg_ID> --changeset <changeset_ID> --accept-patch <patch_ID> [--state <DIR>]",
            writes(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_changeset_object",
                    "read_review",
                    "read_patch_request",
                    "write_private_patch_transaction",
                    "write_dataset",
                    "write_record_state_object",
                    "write_changeset_object",
                    "write_patch_receipt",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "request",
                    "path",
                    true,
                    false,
                    None,
                    "Exact schema-1 request previously passed to patch-preview.",
                ),
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Exact staged review previously passed to patch-preview.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    true,
                    false,
                    None,
                    "Task declaration bound to the accepted patch.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    true,
                    false,
                    None,
                    "Staged candidate bound to the accepted patch.",
                ),
                input(
                    "accept_patch",
                    "content_id:patch",
                    true,
                    false,
                    None,
                    "Exact patch identity returned by patch-preview; explicit authorization boundary.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory containing the staged candidate.",
                ),
            ],
            output("jsonl_patch_applied", "stdout", true),
            vec![
                exit(0, "repair applied or an identical retry recovered"),
                exit(
                    2,
                    "patch not authorized, stale, conflicting, busy, unsafe, or unreadable",
                ),
                exit(4, "finding not found"),
            ],
        ),
        command(
            "patch-draft",
            "Create a bounded field patch request directly from fresh staged finding evidence.",
            "datajig patch-draft <REPORT> <FINDING_ID> --change <chg_ID> --changeset <changeset_ID> (--after-json <JSON>|--remove) --output <FILE> [--record <rid_ID>] [--state <DIR>]",
            writes(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_changeset_object",
                    "read_review",
                    "write_patch_request",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Deterministic staged JSONL review returned by check.",
                ),
                input(
                    "finding_id",
                    "content_id:fnd",
                    true,
                    false,
                    None,
                    "Patchable policy finding to repair.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    true,
                    false,
                    None,
                    "Task declaration bound to the review.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    true,
                    false,
                    None,
                    "Staged candidate bound to the review.",
                ),
                input(
                    "record",
                    "content_id:rid",
                    false,
                    false,
                    None,
                    "Sampled record identity, required only when the finding covers multiple records.",
                ),
                input(
                    "after_json",
                    "json_scalar",
                    false,
                    false,
                    None,
                    "Replacement value; mutually exclusive with remove.",
                ),
                input(
                    "remove",
                    "boolean",
                    false,
                    false,
                    Some("false"),
                    "Remove the field; mutually exclusive with after_json.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "New file for the generated schema-1 patch request.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory containing the staged candidate.",
                ),
            ],
            output("jsonl_patch_request_drafted", "stdout", true),
            vec![
                exit(0, "fresh evidence-bound patch request written"),
                exit(
                    2,
                    "invalid proposal, ambiguous target, stale evidence, or unsafe output",
                ),
                exit(4, "finding not found"),
            ],
        ),
        command(
            "patch-preview",
            "Verify a proposed top-level JSONL field repair against exact staged finding evidence without changing the dataset.",
            "datajig patch-preview <REQUEST> <REPORT> --change <chg_ID> --changeset <changeset_ID> [--state <DIR>]",
            read_only(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_changeset_object",
                    "read_review",
                    "read_patch_request",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "request",
                    "path",
                    true,
                    false,
                    None,
                    "Bounded schema-1 JSONL field patch request.",
                ),
                input(
                    "report",
                    "path",
                    true,
                    false,
                    None,
                    "Deterministic staged JSONL review returned by check.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    true,
                    false,
                    None,
                    "Task declaration bound to the review.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    true,
                    false,
                    None,
                    "Staged JSONL candidate bound to the review.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory containing the immutable candidate state.",
                ),
            ],
            output("jsonl_patch_preview", "stdout", true),
            vec![
                exit(0, "proposed repair is bound to fresh staged evidence"),
                exit(
                    2,
                    "invalid, stale, unsupported, or unreadable patch evidence",
                ),
                exit(4, "finding not found"),
            ],
        ),
        command(
            "patch-undo",
            "Restore the exact source bytes replaced by one guarded JSONL patch while its HEAD anchor remains unchanged.",
            "datajig patch-undo <undo_ID> [--state <DIR>]",
            writes(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_private_patch_transaction",
                    "write_private_patch_transaction",
                    "write_dataset",
                    "write_patch_receipt",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "undo_id",
                    "opaque_handle:undo",
                    true,
                    false,
                    None,
                    "Opaque undo identity returned by patch-apply.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory containing the private patch transaction.",
                ),
            ],
            output("jsonl_patch_undone", "stdout", true),
            vec![
                exit(
                    0,
                    "exact source bytes restored or an identical retry recovered",
                ),
                exit(
                    2,
                    "source, HEAD, transaction, or workspace state conflicts with undo",
                ),
                exit(4, "undo transaction not found"),
            ],
        ),
        command(
            "plan",
            "Turn the latest workspace review into a deterministic Agent action plan.",
            "datajig plan [--state <DIR>] [--change <chg_ID> --changeset <changeset_ID>]",
            writes(
                vec![
                    "read_dataset",
                    "read_workspace",
                    "read_changeset_object",
                    "read_review",
                    "write_remediation_plan",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory containing the latest review.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    false,
                    false,
                    None,
                    "Optional task declaration or alias; omit both selectors to resolve the unique active pair; requires changeset when supplied.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    false,
                    false,
                    None,
                    "Optional staged candidate or alias; omit both selectors to resolve the unique active pair; requires change when supplied.",
                ),
            ],
            output("remediation_plan_created", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "prepare-apply",
            "Re-execute and atomically publish one exact deterministic preparation plan.",
            "datajig prepare-apply <PLAN> --accept-plan <prep_ID>",
            writes(
                vec![
                    "read_dataset",
                    "read_prepare_recipe",
                    "read_prepare_plan",
                    "write_dataset",
                    "write_prepare_receipt",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "plan",
                    "path",
                    true,
                    false,
                    None,
                    "Bounded plan artifact created by prepare-plan.",
                ),
                input(
                    "accept_plan",
                    "content_id:prep",
                    true,
                    false,
                    None,
                    "Exact plan identity returned by prepare-plan; explicit authorization boundary.",
                ),
            ],
            output("prepare_applied", "stdout", true),
            vec![
                exit(0, "predicted dataset and provenance receipt published"),
                exit(
                    2,
                    "plan not authorized, source stale, data invalid, or destination unsafe",
                ),
            ],
        ),
        command(
            "prepare-plan",
            "Preview deterministic CSV, flat Parquet, or JSONL preparation from one file or verified import receipt and bind its predicted keyed JSONL output.",
            "datajig prepare-plan <SOURCE|datajig.hf-import.json> --recipe <RECIPE> --output <DATASET.jsonl> --plan <PLAN.json>",
            writes(
                vec!["read_dataset", "read_prepare_recipe", "write_prepare_plan"],
                unix_platforms(),
            ),
            vec![
                input(
                    "source",
                    "path",
                    true,
                    false,
                    None,
                    "Headered UTF-8 CSV, flat Parquet, JSONL, or verified datajig.hf-import.json source.",
                ),
                input(
                    "recipe",
                    "path",
                    true,
                    false,
                    None,
                    "Schema-1 deterministic preparation recipe.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "New keyed JSONL destination bound into the plan.",
                ),
                input(
                    "plan",
                    "path",
                    true,
                    false,
                    None,
                    "New file for the content-bound preparation plan.",
                ),
            ],
            output("prepare_planned", "stdout", true),
            vec![
                exit(0, "plan and predicted output identity created"),
                exit(2, "recipe, source data, paths, or destination are invalid"),
            ],
        ),
        command(
            "record-diff",
            "Compare two ready JSONL files by typed record identity.",
            "datajig record-diff <BEFORE.jsonl> <AFTER.jsonl> [--id-field <FIELD>]",
            read_only(vec!["read_dataset"], all_platforms()),
            vec![
                input("before", "path", true, false, None, "Baseline JSONL file."),
                input("after", "path", true, false, None, "Candidate JSONL file."),
                input(
                    "id_field",
                    "string",
                    false,
                    false,
                    Some("id"),
                    "Top-level string or number field used as record identity.",
                ),
            ],
            output("record_diff", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "repository-check",
            "Verify repository-managed assets, the pinned Agent contract, hook activation, and clean bound workspaces.",
            "datajig repository-check [--root <REPOSITORY>] [--ci]",
            read_only(
                vec![
                    "read_git_repository",
                    "read_repository_lock",
                    "read_agent_configuration",
                    "read_ci_configuration",
                    "read_git_hook_configuration",
                    "read_bound_workspaces",
                    "read_dataset",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "root",
                    "path",
                    false,
                    false,
                    Some("."),
                    "Git worktree or a directory inside it.",
                ),
                input(
                    "ci",
                    "boolean",
                    false,
                    false,
                    Some("false"),
                    "Skip only the clone-local core.hooksPath assertion.",
                ),
            ],
            output("repository_integration_checked", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "repository-install",
            "Install or safely upgrade the repository-local DataJig Agent, CI, and Git-hook contract.",
            "datajig repository-install [--root <REPOSITORY>] [--state <RELATIVE_PATH>]... [--no-hook] [--no-github-actions]",
            writes(
                vec![
                    "read_git_repository",
                    "write_agent_configuration",
                    "write_ci_configuration",
                    "write_git_hook_configuration",
                    "write_repository_lock",
                ],
                unix_platforms(),
            ),
            vec![
                input(
                    "root",
                    "path",
                    false,
                    false,
                    Some("."),
                    "Git worktree or a directory inside it.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    true,
                    None,
                    "Repository-relative DataJig workspace state path; repeat for multiple states.",
                ),
                input(
                    "no_hook",
                    "boolean",
                    false,
                    false,
                    Some("false"),
                    "Skip the repository-managed pre-commit hook.",
                ),
                input(
                    "no_github_actions",
                    "boolean",
                    false,
                    false,
                    Some("false"),
                    "Skip the repository-managed GitHub Actions workflow.",
                ),
            ],
            output("repository_integration_installed", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "review",
            "Compare two path or inventory references and write a native schema-1 semantic review.",
            "datajig review <BEFORE_REF> <AFTER_REF> --output <REPORT> [--threads <1..8>] [--phash-threshold <0..64>]",
            writes(
                vec!["read_dataset_or_artifact", "write_artifact"],
                unix_platforms(),
            ),
            vec![
                input(
                    "before_ref",
                    "dataset_ref:path|inventory",
                    true,
                    false,
                    None,
                    "Baseline path:<directory>, inventory:<file>, or bare directory path.",
                ),
                input(
                    "after_ref",
                    "dataset_ref:path|inventory",
                    true,
                    false,
                    None,
                    "Candidate path:<directory>, inventory:<file>, or bare directory path.",
                ),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "Destination for the complete schema-1 review report.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Path-reference worker count; valid range is 1 through 8.",
                ),
                input(
                    "phash_threshold",
                    "integer",
                    false,
                    false,
                    Some("6"),
                    "Maximum 64-bit pHash Hamming distance; valid range is 0 through 64.",
                ),
            ],
            output("review_created", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "seal",
            "Accept a fresh PASS review and publish an immutable dataset revision.",
            "datajig seal [--state <DIR>] [--threads <1..8>] [--message <TEXT>] [--accept-report <review_ID>] [--change <chg_ID> --changeset <changeset_ID>]",
            writes(
                vec!["read_dataset", "read_review", "write_revision", "write_ref"],
                unix_platforms(),
            ),
            vec![
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory created by init.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Freshness-verification worker count; valid range is 1 through 8.",
                ),
                input(
                    "message",
                    "string",
                    false,
                    false,
                    Some("Accept dataset revision"),
                    "Human-readable reason stored in revision provenance.",
                ),
                input(
                    "accept_report",
                    "content_id:review",
                    false,
                    false,
                    None,
                    "Exact latest review ID required when a passing review contains findings.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    false,
                    false,
                    None,
                    "Optional task declaration or alias; omit both selectors to resolve the unique active pair; requires changeset when supplied.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    false,
                    false,
                    None,
                    "Optional staged candidate or alias; omit both selectors to resolve the unique active pair; requires change when supplied.",
                ),
            ],
            output("dataset_sealed", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "snapshot",
            "Create a portable content-addressed manifest for a directory.",
            "datajig snapshot <ROOT> --output <OUTPUT> [--threads <N>]",
            writes(vec!["read_dataset", "write_artifact"], unix_platforms()),
            vec![
                input("root", "path", true, false, None, "Directory to snapshot."),
                input(
                    "output",
                    "path",
                    true,
                    false,
                    None,
                    "Destination for the complete manifest JSON artifact.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Content-hashing worker count.",
                ),
            ],
            output("snapshot_created", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "snapshot-diff",
            "Compare two snapshot manifests with bounded pagination.",
            "datajig snapshot-diff <BEFORE> <AFTER> [--offset <N>] [--limit <1..200>]",
            read_only(vec!["read_artifact"], all_platforms()),
            vec![
                input(
                    "before",
                    "path",
                    true,
                    false,
                    None,
                    "Baseline snapshot manifest.",
                ),
                input(
                    "after",
                    "path",
                    true,
                    false,
                    None,
                    "Candidate snapshot manifest.",
                ),
                input(
                    "offset",
                    "integer",
                    false,
                    false,
                    Some("0"),
                    "Zero-based change offset.",
                ),
                input(
                    "limit",
                    "integer",
                    false,
                    false,
                    Some("50"),
                    "Maximum changes to return; valid range is 1 through 200.",
                ),
            ],
            output("snapshot_diff_page", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "snapshot-info",
            "Read a snapshot manifest summary.",
            "datajig snapshot-info <MANIFEST>",
            read_only(vec!["read_artifact"], all_platforms()),
            vec![input(
                "manifest",
                "path",
                true,
                false,
                None,
                "Snapshot manifest JSON file.",
            )],
            output("snapshot_info", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "status",
            "Inspect HEAD and current dataset identity without writing review artifacts.",
            "datajig status [--state <DIR>] [--threads <1..8>] [--change <chg_ID> --changeset <changeset_ID>]",
            read_only(vec!["read_dataset", "read_workspace"], unix_platforms()),
            vec![
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Workspace state directory created by init.",
                ),
                input(
                    "threads",
                    "integer",
                    false,
                    false,
                    Some("1"),
                    "Worker count; valid range is 1 through 8.",
                ),
                input(
                    "change",
                    "content_id:chg",
                    false,
                    false,
                    None,
                    "Optional task declaration or alias; omit both selectors to inspect the unique active pair; requires changeset when supplied.",
                ),
                input(
                    "changeset",
                    "content_id:changeset",
                    false,
                    false,
                    None,
                    "Optional staged candidate or alias; omit both selectors to inspect the unique active pair; requires change when supplied.",
                ),
            ],
            output("workspace_status", "stdout", true),
            standard_read_exits(),
        ),
        command(
            "tutorial",
            "Create and run a complete verified keyed-JSONL-to-training example.",
            "datajig tutorial <OUTPUT>",
            writes(
                vec![
                    "write_tutorial_dataset",
                    "write_workspace",
                    "write_review",
                    "write_revision",
                    "write_training_bundle",
                ],
                unix_platforms(),
            ),
            vec![input(
                "output",
                "path",
                true,
                false,
                None,
                "New directory for the example dataset, workspace state, and verified bundle.",
            )],
            output("tutorial_completed", "stdout", true),
            standard_write_exits(),
        ),
        command(
            "view-check",
            "Resolve a deterministic subset recipe against clean sealed keyed JSONL HEAD.",
            "datajig view-check --recipe <RECIPE> [--state <DIR>]",
            read_only(
                vec!["read_dataset", "read_workspace", "read_subset_recipe"],
                unix_platforms(),
            ),
            vec![
                input(
                    "recipe",
                    "path",
                    true,
                    false,
                    None,
                    "Schema-1 subset recipe containing bounded top-level AND predicates and optional stable sampling.",
                ),
                input(
                    "state",
                    "path",
                    false,
                    false,
                    Some(".datajig"),
                    "Clean sealed keyed-JSONL workspace state directory.",
                ),
            ],
            output("subset_view_checked", "stdout", true),
            vec![
                exit(0, "view resolved; inspect artifact.exportable"),
                exit(
                    2,
                    "invalid recipe, dirty workspace, source race, or I/O failure",
                ),
            ],
        ),
    ];
    debug_assert!(commands.windows(2).all(|pair| pair[0].name < pair[1].name));
    commands
}

pub fn command_names() -> Vec<&'static str> {
    command_catalog()
        .into_iter()
        .filter(command_available_on_current_platform)
        .map(|command| command.name)
        .collect()
}

pub fn command_descriptor(name: &str) -> Option<CommandDescriptor> {
    command_catalog()
        .into_iter()
        .find(|command| command.name == name)
}

pub fn render_agent_skill() -> String {
    let mut rendered = String::from(
        "---\nname: datajig\ndescription: Manage a local dataset workspace with stable identity, immutable revisions, semantic validation, and bounded machine-readable commands.\n---\n\n# DataJig CLI\n\n",
    );
    rendered.push_str(&format!(
        "Generated by `datajig-core {}` for Agent API version {AGENT_API_VERSION}. Agent contract `{}`.\n\n",
        env!("CARGO_PKG_VERSION"),
        agent_contract_id(),
    ));
    rendered.push_str(
        "## Operating rules\n\n\
- Run `datajig capabilities` before relying on an optional feature.\n\
- Run `datajig describe [command]` for the versioned input, output, effect, platform, and exit-code contract.\n\
- Run `datajig artifact-schema [jsonl-field-patch|prepare-recipe|repository-integration|subset-view|training-consumption-plan|training-consumption-receipt]` instead of guessing an input artifact shape.\n\
- Use `repository-install` to publish one version-matched Skill, CI workflow, hook, and content-addressed lock; use `repository-check` before agent work and in CI to reject drift or dirty bound workspaces.\n\
- Start every local table with `datajig inspect`: JSONL returns workspace readiness; CSV and flat Parquet return a privacy-safe profile and inline preparation recipe template.\n\
- For CSV, flat Parquet, or JSONL data preparation, pass either one file or a verified `datajig.hf-import.json`; use recipe `include`/`ignore` globs for imported shards, compose ordered transformations, inspect the bounded `prepare-plan`, then pass the exact returned `prep_...` identity to `prepare-apply`.\n\
- For Hugging Face dataset repositories, run `hf-import-plan`, inspect the resolved 40-character commit and selected paths, then pass the exact returned `hfplan_...` identity to `hf-import-apply`; never substitute a moving branch during apply.\n\
- Always parse successful stdout as JSON; commands with `write_artifact` effects also write the requested file.\n\
- Parse failures from stderr; exit code 2 means invalid input or I/O and exit code 4 means a requested entity was not found.\n\
- Treat commands whose descriptor has `read_only: false` as writes and inspect their declared effects first.\n\n\
- Run `inspect <FILE.jsonl>` before editing record datasets; it reports structure and ID integrity without exposing values.\n\
- Use `record-diff <BEFORE.jsonl> <AFTER.jsonl>` to review record-level changes without exposing IDs or values.\n\
- Pin `--policy <PATH>` at JSONL init when quality constraints must gate changes; `changed_only` governs added/modified records while `full` governs every candidate record.\n\
- Use `view-check --recipe <RECIPE>` to resolve a bounded cohort against clean sealed HEAD before committing compute; an empty view is a successful non-exportable result.\n\
- Pass the same recipe to `export --view <RECIPE>`; cite both `view_id` and `bundle_id` so cohort choice and training bytes remain reproducible.\n\
- After a JSONL revision is sealed and clean, use `export --split ... --output <NEW_DIR>` to create deterministic training shards; never export an unstaged candidate.\n\
- Run `export-info <BUNDLE>/datajig.bundle.json --verify` before training and cite the returned `bundle_id` in the experiment.\n\
- Use `consume-plan` plus an accepted `consume_...` ID when training needs durable evidence; a `consumed_...` receipt proves only that every verified split record crossed the named DataJig adapter boundary at least once.\n\
- Prefer `status` before `check`; status is read-only, while check writes the latest review artifact.\n\
- Before editing a policy finding, use `patch-preview` to bind the proposed scalar field repair to the exact report, staged candidate, record, and live source. Apply only by passing its exact `patch_...` identity to `patch-apply`; then follow the returned check action. Keep the opaque `undo_...` handle for exact recovery with `patch-undo`.\n\
- When a PASS review still has findings, inspect every findings page and pass the exact `report_content_id` to `seal --accept-report`.\n\
- Follow `log` pagination while `artifact.has_more` is true; never infer history from a truncated page.\n\
- Use `materialize <rev_ID> --output <NEW.jsonl>` for byte-exact, non-destructive recovery of a reachable JSONL revision; never replace the tracked dataset implicitly.\n\n\
- New workspaces use `all_files_v2`: every regular file path, size, and byte hash is bound to inventory and changeset identity.\n\
- DataJig inventory schema 1 may report `supported_media_v1`: supported image bytes and all path membership are covered, but unchanged-path unsupported-file byte edits are not covered. Workspace schema 1 is unsupported.\n\
## Commands\n\n",
    );
    for descriptor in command_catalog()
        .into_iter()
        .filter(command_available_on_current_platform)
    {
        let effects = if descriptor.effects.is_empty() {
            "none".to_owned()
        } else {
            descriptor.effects.join(", ")
        };
        rendered.push_str(&format!(
            "- `{}` — {} Effects: {}.\n",
            descriptor.usage, descriptor.summary, effects
        ));
    }
    rendered
}

fn command_available_on_current_platform(command: &CommandDescriptor) -> bool {
    let current = if cfg!(target_os = "linux") {
        "linux"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "unsupported"
    };
    command.platforms.contains(&current)
}

pub fn write_agent_skill(output: &Path) -> Result<AgentSkillArtifact> {
    let rendered = render_agent_skill();
    let payload = rendered.as_bytes();
    if payload.len() > MAX_AGENT_SKILL_BYTES {
        bail!("agent skill exceeds {MAX_AGENT_SKILL_BYTES} bytes");
    }
    save_file_atomically(output, payload, "agent skill")?;
    let canonical = output
        .canonicalize()
        .context("cannot resolve generated agent skill")?;
    Ok(AgentSkillArtifact {
        kind: "agent_skill",
        schema_version: AGENT_SKILL_SCHEMA_VERSION,
        content_id: crate::identity::blake3_content_id(
            "skill",
            b"datajig-agent-skill-v1\0",
            payload,
        ),
        bytes: payload.len(),
        output: canonical.to_string_lossy().into_owned(),
    })
}

fn command(
    name: &'static str,
    summary: &'static str,
    usage: &'static str,
    access: AccessDescriptor,
    inputs: Vec<InputDescriptor>,
    output: OutputDescriptor,
    exit_codes: Vec<ExitCodeDescriptor>,
) -> CommandDescriptor {
    CommandDescriptor {
        name,
        summary,
        usage,
        read_only: access.read_only,
        effects: access.effects,
        platforms: access.platforms,
        inputs,
        output,
        exit_codes,
    }
}

fn read_only(effects: Vec<&'static str>, platforms: Vec<&'static str>) -> AccessDescriptor {
    AccessDescriptor {
        read_only: true,
        effects,
        platforms,
    }
}

fn writes(effects: Vec<&'static str>, platforms: Vec<&'static str>) -> AccessDescriptor {
    AccessDescriptor {
        read_only: false,
        effects,
        platforms,
    }
}

fn input(
    name: &'static str,
    kind: &'static str,
    required: bool,
    repeatable: bool,
    default: Option<&'static str>,
    description: &'static str,
) -> InputDescriptor {
    InputDescriptor {
        name,
        kind,
        required,
        repeatable,
        default,
        description,
    }
}

fn output(kind: &'static str, channel: &'static str, bounded: bool) -> OutputDescriptor {
    OutputDescriptor {
        kind,
        channel,
        bounded,
    }
}

fn exit(code: u8, meaning: &'static str) -> ExitCodeDescriptor {
    ExitCodeDescriptor { code, meaning }
}
fn success_only() -> Vec<ExitCodeDescriptor> {
    vec![exit(0, "success")]
}
fn standard_read_exits() -> Vec<ExitCodeDescriptor> {
    vec![exit(0, "success"), exit(2, "invalid argument or artifact")]
}
fn standard_write_exits() -> Vec<ExitCodeDescriptor> {
    vec![
        exit(0, "artifact written"),
        exit(2, "invalid argument or I/O failure"),
    ]
}
fn all_platforms() -> Vec<&'static str> {
    vec!["linux", "macos", "windows"]
}
fn unix_platforms() -> Vec<&'static str> {
    vec!["linux", "macos"]
}
