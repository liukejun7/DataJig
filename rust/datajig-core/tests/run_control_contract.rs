use datajig_core::{
    AuthorizationLevel, AuthorizationStatus, RunConsumptionBinding, RunPlanBinding, authorize_run,
    classify_run_output, compile_dsl, create_run_plan, derive_run_identities,
    fingerprint_run_source, load_run_plan_for_resume, persist_run_plan,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "datajig-run-control-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn compiled() -> datajig_core::CompiledRunTask {
    compile_dsl("from rows.csv source-id-field id export id-field id").unwrap()
}

fn binding(source_content_id: &str) -> RunPlanBinding {
    RunPlanBinding {
        source_content_id: source_content_id.into(),
        engine_version: "datajig-run-engine-v1".into(),
        output: "bundle".into(),
        consumption: None,
    }
}

#[test]
fn intent_plan_and_attempt_identities_have_distinct_stability_domains() {
    let task = compiled();
    let first = derive_run_identities(&task, &binding("src_aaa"), "attempt-one").unwrap();
    let retried = derive_run_identities(&task, &binding("src_aaa"), "attempt-two").unwrap();
    let new_data = derive_run_identities(&task, &binding("src_bbb"), "attempt-three").unwrap();

    assert_eq!(first.intent_id, task.intent_id);
    assert_eq!(first.intent_id, retried.intent_id);
    assert_eq!(first.plan_id, retried.plan_id);
    assert_ne!(first.attempt_id, retried.attempt_id);
    assert_eq!(first.intent_id, new_data.intent_id);
    assert_ne!(first.plan_id, new_data.plan_id);
    assert!(first.plan_id.starts_with("plan_"));
    assert!(first.attempt_id.starts_with("attempt_"));
}

#[test]
fn consumption_has_its_own_identity_without_changing_intent() {
    let task = compiled();
    let plain = derive_run_identities(&task, &binding("src_aaa"), "one").unwrap();
    let mut consumed_binding = binding("src_aaa");
    consumed_binding.consumption = Some(RunConsumptionBinding {
        consumer: "pytorch".into(),
        run_id: "training-001".into(),
        run_dir: "runs/training-001".into(),
    });
    let consumed = derive_run_identities(&task, &consumed_binding, "two").unwrap();

    assert_eq!(plain.intent_id, consumed.intent_id);
    assert_ne!(plain.plan_id, consumed.plan_id);
    assert_eq!(plain.consumption_id, None);
    assert!(
        consumed
            .consumption_id
            .as_deref()
            .is_some_and(|identity| identity.starts_with("consume_"))
    );
}

#[test]
fn yes_authorizes_only_dangerous_operations_and_never_enters_identity() {
    let task = compiled();
    let without_yes = derive_run_identities(&task, &binding("src_aaa"), "one").unwrap();
    let with_yes = derive_run_identities(&task, &binding("src_aaa"), "one").unwrap();
    assert_eq!(without_yes, with_yes);

    assert_eq!(
        authorize_run(AuthorizationLevel::Safe, false).status,
        AuthorizationStatus::Authorized
    );
    assert_eq!(
        authorize_run(AuthorizationLevel::Dangerous, false).status,
        AuthorizationStatus::ConfirmationRequired
    );
    assert_eq!(
        authorize_run(AuthorizationLevel::Dangerous, true).status,
        AuthorizationStatus::Authorized
    );
    assert_eq!(
        authorize_run(AuthorizationLevel::Fatal, true).status,
        AuthorizationStatus::Rejected
    );
}

#[test]
fn identity_inputs_are_bounded_and_actionable() {
    let task = compiled();
    for (binding, message) in [
        (binding(""), "source_content_id"),
        (
            RunPlanBinding {
                output: "".into(),
                ..binding("src_aaa")
            },
            "output",
        ),
    ] {
        let error = derive_run_identities(&task, &binding, "attempt").unwrap_err();
        assert_eq!(error.code, "INVALID_RUN_PLAN");
        assert!(error.message.contains(message));
    }
}

#[test]
fn source_fingerprints_bind_bytes_and_canonical_relative_membership() {
    let root = TempDir::new();
    fs::create_dir(root.path().join("rows")).unwrap();
    fs::write(root.path().join("rows/a.csv"), b"id,value\n1,a\n").unwrap();
    fs::write(root.path().join("rows/b.csv"), b"id,value\n2,b\n").unwrap();

    let first = fingerprint_run_source(root.path(), Path::new("rows")).unwrap();
    let repeated = fingerprint_run_source(root.path(), Path::new("rows")).unwrap();
    assert_eq!(first, repeated);
    assert!(first.starts_with("source_"));

    fs::write(root.path().join("rows/b.csv"), b"id,value\n2,changed\n").unwrap();
    assert_ne!(
        first,
        fingerprint_run_source(root.path(), Path::new("rows")).unwrap()
    );
}

#[test]
fn plans_use_stable_workspaces_and_distinct_attempt_directories() {
    let root = TempDir::new();
    let task = compiled();
    let first = create_run_plan(
        &task,
        &binding("source_aaa"),
        "attempt-one",
        AuthorizationLevel::Safe,
        false,
    )
    .unwrap();
    let second = create_run_plan(
        &task,
        &binding("source_aaa"),
        "attempt-two",
        AuthorizationLevel::Safe,
        false,
    )
    .unwrap();

    let first_layout = persist_run_plan(root.path(), &first).unwrap();
    let second_layout = persist_run_plan(root.path(), &second).unwrap();
    assert_eq!(first_layout.workspace, second_layout.workspace);
    assert_ne!(first_layout.attempt, second_layout.attempt);
    assert!(first_layout.plan.is_file());
    assert!(second_layout.plan.is_file());
    assert_eq!(
        load_run_plan_for_resume(root.path(), &first.attempt_id, &first.plan_id).unwrap(),
        first
    );
}

#[test]
fn resume_rejects_wrong_acceptance_and_tampered_plan_files() {
    let root = TempDir::new();
    let plan = create_run_plan(
        &compiled(),
        &binding("source_aaa"),
        "attempt-one",
        AuthorizationLevel::Safe,
        false,
    )
    .unwrap();
    let layout = persist_run_plan(root.path(), &plan).unwrap();

    let wrong = load_run_plan_for_resume(root.path(), &plan.attempt_id, "plan_wrong").unwrap_err();
    assert_eq!(wrong.code, "RUN_PLAN_MISMATCH");

    let mut payload = fs::read_to_string(&layout.plan).unwrap();
    payload = payload.replace("source_aaa", "source_tampered");
    fs::write(&layout.plan, payload).unwrap();
    let tampered =
        load_run_plan_for_resume(root.path(), &plan.attempt_id, &plan.plan_id).unwrap_err();
    assert_eq!(tampered.code, "RUN_PLAN_CORRUPT");
}

#[test]
fn fatal_output_preflight_never_creates_run_state() {
    let root = TempDir::new();
    fs::create_dir(root.path().join("bundle")).unwrap();
    fs::write(root.path().join("bundle/user.txt"), b"mine").unwrap();
    let decision = classify_run_output(root.path(), Path::new("bundle"), "intent_any").unwrap();
    assert_eq!(decision.level, AuthorizationLevel::Fatal);

    let plan = create_run_plan(
        &compiled(),
        &binding("source_aaa"),
        "attempt-one",
        decision.level,
        true,
    )
    .unwrap();
    let error = persist_run_plan(root.path(), &plan).unwrap_err();
    assert_eq!(error.code, "RUN_AUTHORIZATION_REJECTED");
    assert!(!root.path().join(".datajig").exists());
}

#[cfg(unix)]
#[test]
fn source_and_output_symlinks_fail_closed() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new();
    let outside = TempDir::new();
    fs::write(outside.path().join("rows.csv"), b"id\n1\n").unwrap();
    symlink(
        outside.path().join("rows.csv"),
        root.path().join("rows.csv"),
    )
    .unwrap();
    assert!(fingerprint_run_source(root.path(), Path::new("rows.csv")).is_err());

    symlink(outside.path(), root.path().join("bundle")).unwrap();
    let decision = classify_run_output(root.path(), Path::new("bundle"), "intent_any").unwrap();
    assert_eq!(decision.level, AuthorizationLevel::Fatal);
}
