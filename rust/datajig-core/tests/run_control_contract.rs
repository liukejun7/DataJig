use datajig_core::{
    AuthorizationLevel, AuthorizationStatus, RunConsumptionBinding, RunPlanBinding, authorize_run,
    compile_dsl, derive_run_identities,
};

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
