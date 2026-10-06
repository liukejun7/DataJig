use datajig_core::{
    ProviderRequest, TransformInputSpec, TransformLimits, TransformProviderExecutionError,
    TransformProviderIdentity, TransformProviderProtocolError, TransformProviderTimeoutError,
    TransformSourceFormat, execute_transform_provider, probe_transform_provider,
    stage_transform_sources,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[test]
fn staged_sources_are_private_snapshots_and_requests_hide_original_paths() {
    let root = fixture_root("stage");
    let source = root.join("private-events.csv");
    fs::write(&source, "id,score\na,1\nb,2\n").unwrap();
    let sandbox = root.join("sandbox");
    fs::create_dir(&sandbox).unwrap();
    let staged = stage_transform_sources(
        &[
            TransformInputSpec::new("events".into(), source.clone(), TransformSourceFormat::Csv)
                .unwrap(),
        ],
        &sandbox,
        &TransformLimits::v1(),
    )
    .unwrap();

    assert_eq!(2, staged[0].descriptor().rows);
    assert_eq!(
        source.canonicalize().unwrap().to_str().unwrap(),
        staged[0].descriptor().path
    );
    assert!(staged[0].staged_path().starts_with(&sandbox));
    assert_ne!(staged[0].staged_path(), source);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            0o600,
            fs::metadata(staged[0].staged_path())
                .unwrap()
                .permissions()
                .mode()
                & 0o777
        );
    }

    let candidate = sandbox.join("candidate.jsonl");
    let temp = sandbox.join("temp");
    fs::create_dir(&temp).unwrap();
    let request = ProviderRequest::execute(
        "corr-stage".into(),
        provider(),
        &staged,
        "SELECT id, score FROM events ORDER BY id".into(),
        vec![],
        "id".into(),
        candidate,
        temp,
        TransformLimits::v1(),
        "policy-test".into(),
    )
    .unwrap();
    let payload = request.to_json().unwrap();
    assert!(!payload.contains(source.to_str().unwrap()));
    assert!(payload.contains(staged[0].staged_path().to_str().unwrap()));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn provider_request_rejects_paths_outside_the_staging_sandbox() {
    let fixture = provider_fixture("path-escape");
    let temp = fixture.sandbox.join("temp");
    fs::create_dir(&temp).unwrap();
    let error = ProviderRequest::execute(
        "corr-escape".into(),
        provider(),
        &fixture.staged,
        "SELECT id FROM events ORDER BY id".into(),
        vec![],
        "id".into(),
        fixture.root.join("escaped.jsonl"),
        temp,
        TransformLimits::v1(),
        "policy-test".into(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("sandbox"));
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn staging_rejects_symlinks_and_enforces_streamed_limits() {
    let root = fixture_root("limits");
    let source = root.join("events.jsonl");
    fs::write(&source, "{\"id\":\"a\"}\n{\"id\":\"b\"}\n").unwrap();
    let sandbox = root.join("sandbox");
    fs::create_dir(&sandbox).unwrap();

    let mut row_limits = TransformLimits::v1();
    row_limits.source_rows = 1;
    let row_error = stage_transform_sources(
        &[TransformInputSpec::new(
            "events".into(),
            source.clone(),
            TransformSourceFormat::Jsonl,
        )
        .unwrap()],
        &sandbox,
        &row_limits,
    )
    .unwrap_err();
    assert!(
        row_error
            .to_string()
            .contains("at least 2 rows > limit 1 row"),
        "{row_error}"
    );
    assert_eq!(0, fs::read_dir(&sandbox).unwrap().count());

    let mut byte_limits = TransformLimits::v1();
    byte_limits.source_bytes = 4;
    let byte_error = stage_transform_sources(
        &[TransformInputSpec::new(
            "events".into(),
            source.clone(),
            TransformSourceFormat::Jsonl,
        )
        .unwrap()],
        &sandbox,
        &byte_limits,
    )
    .unwrap_err();
    assert!(
        byte_error
            .to_string()
            .contains("at least 5 bytes > limit 4 bytes"),
        "{byte_error}"
    );
    assert_eq!(0, fs::read_dir(&sandbox).unwrap().count());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let link = root.join("events-link.jsonl");
        symlink(&source, &link).unwrap();
        assert!(
            stage_transform_sources(
                &[
                    TransformInputSpec::new("events".into(), link, TransformSourceFormat::Jsonl)
                        .unwrap()
                ],
                &sandbox,
                &TransformLimits::v1(),
            )
            .is_err()
        );

        let real_parent = root.join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        let nested = real_parent.join("nested.jsonl");
        fs::write(&nested, "{\"id\":\"nested\"}\n").unwrap();
        let parent_link = root.join("parent-link");
        symlink(&real_parent, &parent_link).unwrap();
        assert!(
            stage_transform_sources(
                &[TransformInputSpec::new(
                    "events".into(),
                    parent_link.join("nested.jsonl"),
                    TransformSourceFormat::Jsonl,
                )
                .unwrap()],
                &sandbox,
                &TransformLimits::v1(),
            )
            .is_err(),
            "every source path component must be opened without following symlinks"
        );
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn provider_process_accepts_one_bounded_correlated_response() {
    let fixture = provider_fixture("success");
    let response = json!({
        "protocol": "datajig.transform-provider.v1",
        "protocol_version": 1,
        "correlation_id": "corr-success",
        "status": "ok",
        "provider": provider(),
        "schema": [],
        "rows": 0,
        "bytes": 0,
        "candidate_complete": true
    });
    let script = write_provider_script(
        &fixture.root,
        "success.sh",
        &format!(
            "printf '{{}}\\n' > '{}'\nprintf '%s\\n' '{}'\n",
            fixture.candidate.display(),
            shell_quote(&response.to_string())
        ),
    );

    let summary = execute_transform_provider(
        &script,
        &fixture.request("corr-success"),
        Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(0, summary.rows);
    assert_eq!(fixture.candidate, summary.candidate_path);
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn provider_probe_returns_only_a_compatible_lockdown_identity() {
    let root = fixture_root("probe");
    let response = json!({
        "protocol": "datajig.transform-provider.v1",
        "protocol_version": 1,
        "correlation_id": "probe",
        "status": "ok",
        "provider": provider(),
        "lockdown_supported": true
    });
    let script = write_provider_script(
        &root,
        "probe.sh",
        &format!("printf '%s\\n' '{}'\n", shell_quote(&response.to_string())),
    );

    let identity = probe_transform_provider(&script, &TransformLimits::v1()).unwrap();
    assert_eq!(provider(), identity);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn provider_process_rejects_malformed_mismatched_oversized_and_timeout() {
    for (label, body) in [
        ("malformed", "printf 'not-json\\n'\n".to_owned()),
        (
            "mismatch",
            format!(
                "printf '%s\\n' '{}'\n",
                shell_quote(
                    &json!({
                        "protocol": "datajig.transform-provider.v1", "protocol_version": 1,
                        "correlation_id": "wrong", "status": "ok", "provider": provider(),
                        "schema": [], "rows": 0, "bytes": 0, "candidate_complete": true
                    })
                    .to_string()
                )
            ),
        ),
        ("oversized", "head -c 1048577 /dev/zero\n".to_owned()),
    ] {
        let fixture = provider_fixture(label);
        let script = write_provider_script(&fixture.root, &format!("{label}.sh"), &body);
        let error = execute_transform_provider(
            &script,
            &fixture.request("corr-fail"),
            Duration::from_secs(2),
        )
        .unwrap_err();
        assert!(
            error
                .downcast_ref::<TransformProviderProtocolError>()
                .is_some()
        );
        let _ = fs::remove_dir_all(fixture.root);
    }

    let fixture = provider_fixture("timeout");
    let script = write_provider_script(&fixture.root, "timeout.sh", "sleep 2\n");
    let started = Instant::now();
    let error = execute_transform_provider(
        &script,
        &fixture.request("corr-timeout"),
        Duration::from_millis(50),
    )
    .unwrap_err();
    assert!(
        error
            .downcast_ref::<TransformProviderTimeoutError>()
            .is_some()
    );
    assert!(error.to_string().contains("limit 0.05 seconds"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(1));
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn provider_process_preserves_a_valid_structured_failure() {
    let fixture = provider_fixture("structured-error");
    let response = json!({
        "protocol": "datajig.transform-provider.v1",
        "protocol_version": 1,
        "correlation_id": "corr-error",
        "status": "error",
        "error": {
            "code": "SOURCE_DRIFT",
            "message": "The staged source changed.",
            "remediation": "Create a new plan."
        }
    });
    let script = write_provider_script(
        &fixture.root,
        "structured-error.sh",
        &format!(
            "printf '%s\\n' '{}'\nexit 1\n",
            shell_quote(&response.to_string())
        ),
    );
    let error = execute_transform_provider(
        &script,
        &fixture.request("corr-error"),
        Duration::from_secs(2),
    )
    .unwrap_err();
    let provider_error = error
        .downcast_ref::<TransformProviderExecutionError>()
        .expect("structured provider error should be preserved");
    assert_eq!("SOURCE_DRIFT", provider_error.code());
    assert_eq!("Create a new plan.", provider_error.remediation());
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn successful_provider_descendants_are_terminated_before_stream_join() {
    let fixture = provider_fixture("success-descendant");
    let response = json!({
        "protocol": "datajig.transform-provider.v1",
        "protocol_version": 1,
        "correlation_id": "corr-descendant",
        "status": "ok",
        "provider": provider(),
        "schema": [],
        "rows": 0,
        "bytes": 0,
        "candidate_complete": true
    });
    let script = write_provider_script(
        &fixture.root,
        "success-descendant.sh",
        &format!(
            "sleep 2 &\nprintf '{{}}\\n' > '{}'\nprintf '%s\\n' '{}'\n",
            fixture.candidate.display(),
            shell_quote(&response.to_string())
        ),
    );

    let started = Instant::now();
    execute_transform_provider(
        &script,
        &fixture.request("corr-descendant"),
        Duration::from_secs(3),
    )
    .unwrap();

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "successful completion must kill descendants holding protocol pipes"
    );
    let _ = fs::remove_dir_all(fixture.root);
}

struct ProviderFixture {
    root: PathBuf,
    sandbox: PathBuf,
    candidate: PathBuf,
    staged: Vec<datajig_core::StagedTransformSource>,
}

impl ProviderFixture {
    fn request(&self, correlation: &str) -> ProviderRequest {
        let temp = self.sandbox.join("temp");
        fs::create_dir_all(&temp).unwrap();
        ProviderRequest::execute(
            correlation.into(),
            provider(),
            &self.staged,
            "SELECT id FROM events ORDER BY id".into(),
            vec![],
            "id".into(),
            self.candidate.clone(),
            temp,
            TransformLimits::v1(),
            "policy-test".into(),
        )
        .unwrap()
    }
}

fn provider_fixture(label: &str) -> ProviderFixture {
    let root = fixture_root(label);
    let source = root.join("events.csv");
    fs::write(&source, "id\na\n").unwrap();
    let sandbox = root.join("sandbox");
    fs::create_dir(&sandbox).unwrap();
    let staged = stage_transform_sources(
        &[TransformInputSpec::new("events".into(), source, TransformSourceFormat::Csv).unwrap()],
        &sandbox,
        &TransformLimits::v1(),
    )
    .unwrap();
    ProviderFixture {
        candidate: sandbox.join("candidate.jsonl"),
        root,
        sandbox,
        staged,
    }
}

fn provider() -> TransformProviderIdentity {
    TransformProviderIdentity::create(
        "0.6.0".into(),
        "1.5.6".into(),
        "CPython".into(),
        "3.12.14".into(),
    )
    .unwrap()
}

fn write_provider_script(root: &Path, name: &str, body: &str) -> PathBuf {
    let path = root.join(name);
    fs::write(
        &path,
        format!("#!/bin/sh\ndd of=/dev/null 2>/dev/null\n{body}"),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    path
}

fn shell_quote(value: &str) -> String {
    value.replace('\'', "'\"'\"'")
}

fn fixture_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "datajig-transform-provider-{}-{nonce}-{label}",
        std::process::id()
    ));
    fs::create_dir(&path).unwrap();
    path
}
