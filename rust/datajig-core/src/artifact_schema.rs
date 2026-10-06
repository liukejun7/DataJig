use crate::repository_integration::repository_integration_schema_example;
use serde_json::{Value, json};

pub const ARTIFACT_SCHEMA_VERSION: u8 = 1;
type ArtifactSchemaFactory = fn() -> Value;

const ARTIFACT_SCHEMA_REGISTRY: [(&str, ArtifactSchemaFactory); 6] = [
    ("jsonl-field-patch", jsonl_field_patch_artifact),
    ("prepare-recipe", prepare_recipe_artifact),
    ("repository-integration", repository_integration_artifact),
    ("subset-view", subset_view_artifact),
    (
        "training-consumption-plan",
        training_consumption_plan_artifact,
    ),
    (
        "training-consumption-receipt",
        training_consumption_receipt_artifact,
    ),
];

fn repository_integration_artifact() -> Value {
    let lock = repository_integration_schema_example();
    let example: Value = serde_json::from_str(&lock.to_json().expect("lock should serialize"))
        .expect("lock should be JSON");
    json!({
        "name": "repository-integration",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "media_type": "application/json",
        "schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "DataJig repository integration lock",
            "type": "object",
            "additionalProperties": false,
            "required": ["namespace", "kind", "schema_version", "repository_integration_id", "datajig_version", "agent_api_version", "agent_contract_id", "components", "states", "managed_files", "hooks_path"],
            "properties": {
                "namespace": {"const": "datajig-v1"},
                "kind": {"const": "repository_integration"},
                "schema_version": {"const": 1},
                "repository_integration_id": {"type": "string", "pattern": "^repo_[0-9a-f]{64}$"},
                "datajig_version": {"type": "string", "minLength": 1, "maxLength": 128},
                "agent_api_version": {"type": "integer", "minimum": 1},
                "agent_contract_id": {"type": "string", "pattern": "^contract_[0-9a-f]{64}$"},
                "components": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["agent_skill", "github_actions", "hook"],
                    "properties": {
                        "agent_skill": {"const": true},
                        "github_actions": {"type": "boolean"},
                        "hook": {"type": "boolean"}
                    }
                },
                "states": {
                    "type": "array",
                    "maxItems": 64,
                    "uniqueItems": true,
                    "items": {"type": "string", "minLength": 1}
                },
                "managed_files": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 3,
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["path", "content_id", "bytes", "executable"],
                        "properties": {
                            "path": {"enum": [".agents/skills/datajig/SKILL.md", ".github/workflows/datajig.yml", ".githooks/pre-commit"]},
                            "content_id": {"type": "string", "pattern": "^managed_[0-9a-f]{64}$"},
                            "bytes": {"type": "integer", "minimum": 1},
                            "executable": {"type": "boolean"}
                        }
                    }
                },
                "hooks_path": {"type": ["string", "null"]}
            }
        },
        "example": example
    })
}

pub fn artifact_schema_names() -> Vec<&'static str> {
    ARTIFACT_SCHEMA_REGISTRY
        .iter()
        .map(|(name, _)| *name)
        .collect()
}

pub fn artifact_schema(name: &str) -> Option<Value> {
    ARTIFACT_SCHEMA_REGISTRY
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, factory)| factory())
}

fn jsonl_field_patch_artifact() -> Value {
    json!({
        "name": "jsonl-field-patch",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "media_type": "application/json",
        "schema": jsonl_field_patch_schema(),
        "example": {
            "namespace": "datajig",
            "kind": "jsonl_field_patch",
            "schema_version": 1,
            "report_content_id": format!("review_{}", "0".repeat(64)),
            "candidate_state_id": format!("recordstate_{}", "0".repeat(64)),
            "finding_id": "fnd_00000000000000000000_0001",
            "record_id": format!("rid_{}", "0".repeat(64)),
            "record_content_id": format!("record_{}", "0".repeat(64)),
            "before": {"present": true, "value": 1.2},
            "after": {"present": true, "value": 0.9}
        }
    })
}

fn prepare_recipe_artifact() -> Value {
    json!({
        "name": "prepare-recipe",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "media_type": "application/json",
        "schema": prepare_recipe_schema(),
        "example": {
            "namespace": "datajig",
            "kind": "prepare",
            "schema_version": 1,
            "source": {"format": "csv", "delimiter": ","},
            "output": {"format": "jsonl"},
            "id_field": "id",
            "steps": [
                {"op": "trim", "fields": ["status", "score"]},
                {"op": "replace", "field": "score", "from": "N/A", "to": null},
                {"op": "fill_missing", "field": "score", "value": "0"},
                {"op": "filter", "field": "status", "predicate": "eq", "value": "active"},
                {"op": "cast", "field": "score", "type": "number"},
                {"op": "dedupe", "by": ["id"], "keep": "first"}
            ]
        }
    })
}

fn subset_view_artifact() -> Value {
    json!({
        "name": "subset-view",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "media_type": "application/json",
        "schema": subset_view_schema(),
        "example": {
            "namespace": "datajig",
            "kind": "subset_view",
            "schema_version": 1,
            "where": [
                {"field": "status", "op": "eq", "value": "published"}
            ]
        }
    })
}

fn training_consumption_plan_artifact() -> Value {
    json!({
        "name": "training-consumption-plan",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "media_type": "application/json",
        "schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "DataJig training consumption plan",
            "type": "object",
            "additionalProperties": false,
            "required": ["namespace", "kind", "schema_version", "consumption_plan_id", "run_id", "consumer", "manifest_path", "run_dir", "bundle_id", "source", "split", "records", "bytes", "shards"],
            "properties": {
                "namespace": {"const": "datajig"},
                "kind": {"const": "training_consumption_plan"},
                "schema_version": {"const": 1},
                "consumption_plan_id": {"type": "string", "pattern": "^consume_[0-9a-f]{64}$"},
                "run_id": {"type": "string", "minLength": 1, "maxLength": 256},
                "consumer": {"enum": ["python", "pytorch", "huggingface"]},
                "manifest_path": {"type": "string", "minLength": 1},
                "run_dir": {"type": "string", "minLength": 1},
                "bundle_id": {"type": "string", "pattern": "^bundle_[0-9a-f]{64}$"},
                "source": {"type": "object"},
                "split": {"type": "string", "minLength": 1},
                "records": {"type": "integer", "minimum": 1},
                "bytes": {"type": "integer", "minimum": 1},
                "shards": {"type": "array", "minItems": 1, "maxItems": 4096}
            }
        },
        "example": consumption_plan_example()
    })
}

fn training_consumption_receipt_artifact() -> Value {
    json!({
        "name": "training-consumption-receipt",
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "media_type": "application/json",
        "schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "DataJig training consumption receipt",
            "type": "object",
            "additionalProperties": false,
            "required": ["namespace", "kind", "schema_version", "consumption_receipt_id", "consumption_plan_id", "run_id", "consumer", "bundle_id", "split", "records", "shards", "claim", "plan", "run_dir"],
            "properties": {
                "namespace": {"const": "datajig"},
                "kind": {"const": "training_consumption_receipt"},
                "schema_version": {"const": 1},
                "consumption_receipt_id": {"type": "string", "pattern": "^consumed_[0-9a-f]{64}$"},
                "consumption_plan_id": {"type": "string", "pattern": "^consume_[0-9a-f]{64}$"},
                "run_id": {"type": "string", "minLength": 1, "maxLength": 256},
                "consumer": {"enum": ["python", "pytorch", "huggingface"]},
                "bundle_id": {"type": "string", "pattern": "^bundle_[0-9a-f]{64}$"},
                "split": {"type": "string", "minLength": 1},
                "records": {"type": "integer", "minimum": 1},
                "shards": {"type": "integer", "minimum": 1},
                "claim": {"const": "all_verified_split_records_crossed_adapter_boundary_at_least_once"},
                "plan": {"type": "string", "minLength": 1},
                "run_dir": {"type": "string", "minLength": 1}
            }
        },
        "example": {
            "namespace": "datajig",
            "kind": "training_consumption_receipt",
            "schema_version": 1,
            "consumption_receipt_id": format!("consumed_{}", "0".repeat(64)),
            "consumption_plan_id": format!("consume_{}", "0".repeat(64)),
            "run_id": "train-2026-10-06-001",
            "consumer": "pytorch",
            "bundle_id": format!("bundle_{}", "0".repeat(64)),
            "split": "train",
            "records": 2,
            "shards": 1,
            "claim": "all_verified_split_records_crossed_adapter_boundary_at_least_once",
            "plan": "/workspace/artifacts/train.consume.json",
            "run_dir": "/workspace/runs/train-2026-10-06-001"
        }
    })
}

fn consumption_plan_example() -> Value {
    json!({
        "namespace": "datajig",
        "kind": "training_consumption_plan",
        "schema_version": 1,
        "consumption_plan_id": format!("consume_{}", "0".repeat(64)),
        "run_id": "train-2026-10-06-001",
        "consumer": "pytorch",
        "manifest_path": "/workspace/bundle/datajig.bundle.json",
        "run_dir": "/workspace/runs/train-2026-10-06-001",
        "bundle_id": format!("bundle_{}", "0".repeat(64)),
        "source": {
            "dataset_id": format!("ds_{}", "0".repeat(64)),
            "revision_id": format!("rev_{}", "0".repeat(64)),
            "state_id": format!("recordstate_{}", "0".repeat(64)),
            "dataset_content_id": format!("records_{}", "0".repeat(64)),
            "id_field": "id",
            "assurance": "structural"
        },
        "split": "train",
        "records": 2,
        "bytes": 42,
        "shards": [{
            "relative_path": "train/part-00000.jsonl",
            "split": "train",
            "index": 0,
            "records": 2,
            "bytes": 42,
            "shard_id": format!("shard_{}", "0".repeat(64))
        }]
    })
}

fn prepare_field_schema() -> Value {
    json!({"type": "string", "minLength": 1, "maxLength": 1024})
}

fn prepare_step_schema() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "field", "predicate", "value"],
                "properties": {
                    "op": {"const": "filter"},
                    "field": prepare_field_schema(),
                    "predicate": {"enum": ["eq", "ne", "lt", "lte", "gt", "gte"]},
                    "value": json_scalar_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "fields"],
                "properties": {
                    "op": {"const": "select"},
                    "fields": {
                        "type": "array", "minItems": 1, "maxItems": 1024,
                        "uniqueItems": true, "items": prepare_field_schema()
                    }
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "from", "to"],
                "properties": {
                    "op": {"const": "rename"},
                    "from": prepare_field_schema(),
                    "to": prepare_field_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "field", "type"],
                "properties": {
                    "op": {"const": "cast"},
                    "field": prepare_field_schema(),
                    "type": {"enum": ["string", "integer", "number", "boolean"]}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "fields"],
                "properties": {
                    "op": {"const": "trim"},
                    "fields": prepare_fields_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "fields", "mode"],
                "properties": {
                    "op": {"const": "case"},
                    "fields": prepare_fields_schema(),
                    "mode": {"enum": ["lower", "upper"]}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "field", "from", "to"],
                "properties": {
                    "op": {"const": "replace"},
                    "field": prepare_field_schema(),
                    "from": json_scalar_schema(),
                    "to": json_scalar_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "field", "value"],
                "properties": {
                    "op": {"const": "fill_missing"},
                    "field": prepare_field_schema(),
                    "value": json_scalar_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "fields", "mode"],
                "properties": {
                    "op": {"const": "drop_missing"},
                    "fields": prepare_fields_schema(),
                    "mode": {"enum": ["any", "all"]}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["op", "by", "keep"],
                "properties": {
                    "op": {"const": "dedupe"},
                    "by": {
                        "type": "array", "minItems": 1, "maxItems": 1024,
                        "uniqueItems": true, "items": prepare_field_schema()
                    },
                    "keep": {"enum": ["first", "error"]}
                }
            }
        ]
    })
}

fn prepare_fields_schema() -> Value {
    json!({
        "type": "array", "minItems": 1, "maxItems": 1024,
        "uniqueItems": true, "items": prepare_field_schema()
    })
}

fn prepare_recipe_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "DataJig deterministic preparation recipe",
        "type": "object",
        "additionalProperties": false,
        "required": ["namespace", "kind", "schema_version", "source", "output", "id_field", "steps"],
        "properties": {
            "namespace": {"const": "datajig"},
            "kind": {"const": "prepare"},
            "schema_version": {"const": 1},
            "source": {
                "oneOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["format"],
                        "properties": {
                            "format": {"const": "csv"},
                            "delimiter": {"type": "string", "minLength": 1, "maxLength": 1, "default": ","},
                            "include": prepare_source_patterns_schema(),
                            "ignore": prepare_source_patterns_schema()
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["format"],
                        "properties": {
                            "format": {"const": "parquet"},
                            "include": prepare_source_patterns_schema(),
                            "ignore": prepare_source_patterns_schema()
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["format"],
                        "properties": {
                            "format": {"const": "jsonl"},
                            "include": prepare_source_patterns_schema(),
                            "ignore": prepare_source_patterns_schema()
                        }
                    }
                ]
            },
            "output": {
                "type": "object",
                "additionalProperties": false,
                "required": ["format"],
                "properties": {"format": {"const": "jsonl"}}
            },
            "id_field": prepare_field_schema(),
            "steps": {
                "type": "array",
                "maxItems": 64,
                "items": prepare_step_schema()
            }
        }
    })
}

fn prepare_source_patterns_schema() -> Value {
    json!({
        "type": "array",
        "maxItems": 64,
        "default": [],
        "items": {"type": "string", "minLength": 1, "maxLength": 1024}
    })
}

fn json_scalar_schema() -> Value {
    json!({"type": ["string", "number", "boolean", "null"]})
}

fn patch_value_schema() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["present", "value"],
                "properties": {
                    "present": {"const": true},
                    "value": json_scalar_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["present", "value"],
                "properties": {
                    "present": {"const": false},
                    "value": {"const": null}
                }
            }
        ]
    })
}

fn jsonl_field_patch_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "DataJig JSONL field patch request",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "namespace", "kind", "schema_version", "report_content_id",
            "candidate_state_id", "finding_id", "record_id", "record_content_id",
            "before", "after"
        ],
        "properties": {
            "namespace": {"const": "datajig"},
            "kind": {"const": "jsonl_field_patch"},
            "schema_version": {"const": 1},
            "report_content_id": {"type": "string", "pattern": "^review_[0-9a-f]{64}$"},
            "candidate_state_id": {"type": "string", "pattern": "^recordstate_[0-9a-f]{64}$"},
            "finding_id": {"type": "string", "pattern": "^fnd_[0-9a-f]{20}_[0-9]{4}$"},
            "record_id": {"type": "string", "pattern": "^rid_[0-9a-f]{64}$"},
            "record_content_id": {"type": "string", "pattern": "^record_[0-9a-f]{64}$"},
            "before": patch_value_schema(),
            "after": patch_value_schema()
        }
    })
}

fn subset_clause_schema() -> Value {
    json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["field", "op"],
                "properties": {
                    "field": {"type": "string", "minLength": 1},
                    "op": {"enum": ["exists", "missing", "is_null"]}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["field", "op", "value"],
                "properties": {
                    "field": {"type": "string", "minLength": 1},
                    "op": {"const": "eq"},
                    "value": json_scalar_schema()
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["field", "op", "values"],
                "properties": {
                    "field": {"type": "string", "minLength": 1},
                    "op": {"const": "in"},
                    "values": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 256,
                        "items": json_scalar_schema()
                    }
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["field", "op", "value"],
                "properties": {
                    "field": {"type": "string", "minLength": 1},
                    "op": {"enum": ["lt", "lte", "gt", "gte"]},
                    "value": {"type": "number"}
                }
            }
        ]
    })
}

fn subset_view_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "DataJig JSONL subset view recipe",
        "type": "object",
        "additionalProperties": false,
        "required": ["namespace", "kind", "schema_version"],
        "properties": {
            "namespace": {"const": "datajig"},
            "kind": {"const": "subset_view"},
            "schema_version": {"const": 1},
            "where": {
                "type": "array",
                "maxItems": 64,
                "items": subset_clause_schema()
            },
            "sample": {
                "type": "object",
                "additionalProperties": false,
                "required": ["rate_bps", "seed"],
                "properties": {
                    "rate_bps": {"type": "integer", "minimum": 1, "maximum": 10000},
                    "seed": {"type": "string", "minLength": 1, "maxLength": 256}
                }
            }
        }
    })
}
