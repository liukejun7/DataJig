use datajig_core::{
    RepositoryComponents, RepositoryIntegrationLock, build_repository_integration_lock,
    check_repository, initialize_jsonl_workspace, install_repository, render_agent_skill,
    render_repository_hook, render_repository_workflow,
};
use serde_json::Value;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn repository_lock_is_deterministic_and_relocation_stable() {
    let states = vec![
        PathBuf::from("datasets/alpha/.datajig"),
        PathBuf::from("研究/.datajig"),
    ];
    let components = RepositoryComponents {
        hook: true,
        github_actions: true,
    };

    let first = build_repository_integration_lock(&states, components).unwrap();
    let second = build_repository_integration_lock(&states, components).unwrap();
    let payload = first.to_json().unwrap();
    let reparsed = RepositoryIntegrationLock::from_json(&payload).unwrap();

    assert_eq!(first.integration_id(), second.integration_id());
    assert_eq!(first.integration_id(), reparsed.integration_id());
    assert_eq!(payload, reparsed.to_json().unwrap());
    assert!(first.integration_id().starts_with("repo_"));
    assert_eq!(69, first.integration_id().len());

    let document: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!("datajig", document["namespace"]);
    assert_eq!("repository_integration", document["kind"]);
    assert_eq!(1, document["schema_version"]);
    assert_eq!(
        ["datasets/alpha/.datajig", "研究/.datajig"],
        document["states"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect::<Vec<_>>()
            .as_slice()
    );
    assert!(
        !payload.contains("/tmp/"),
        "lock identity must not include a machine-specific root"
    );
}

#[test]
fn repository_lock_rejects_duplicate_unknown_tampered_and_noncanonical_state() {
    let lock = build_repository_integration_lock(
        &[PathBuf::from(".datajig")],
        RepositoryComponents {
            hook: true,
            github_actions: true,
        },
    )
    .unwrap();
    let original = lock.to_json().unwrap();
    let duplicate = original.replacen("{", "{\"namespace\":\"datajig\",", 1);
    assert!(RepositoryIntegrationLock::from_json(&duplicate).is_err());

    let mut unknown: Value = serde_json::from_str(&original).unwrap();
    unknown["unknown"] = Value::Bool(true);
    assert!(
        RepositoryIntegrationLock::from_json(&serde_json::to_string(&unknown).unwrap()).is_err()
    );

    let mut tampered: Value = serde_json::from_str(&original).unwrap();
    tampered["states"] = serde_json::json!(["other/.datajig"]);
    assert!(
        RepositoryIntegrationLock::from_json(&serde_json::to_string(&tampered).unwrap()).is_err()
    );

    for invalid in ["", ".", "../state", "/tmp/state", "a/../state", "a//state"] {
        assert!(
            build_repository_integration_lock(
                &[PathBuf::from(invalid)],
                RepositoryComponents {
                    hook: false,
                    github_actions: false,
                },
            )
            .is_err(),
            "accepted invalid state path {invalid:?}"
        );
    }
    assert!(
        build_repository_integration_lock(
            &[PathBuf::from("state"), PathBuf::from("state")],
            RepositoryComponents {
                hook: false,
                github_actions: false,
            },
        )
        .is_err()
    );
}

#[test]
fn managed_assets_pin_one_contract_and_exact_package_version() {
    let lock = build_repository_integration_lock(
        &[],
        RepositoryComponents {
            hook: true,
            github_actions: true,
        },
    )
    .unwrap();
    let document: Value = serde_json::from_str(&lock.to_json().unwrap()).unwrap();
    let managed = document["managed_files"].as_array().unwrap();
    let paths = managed
        .iter()
        .map(|file| file["path"].as_str().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        vec![
            ".agents/skills/datajig/SKILL.md",
            ".github/workflows/datajig.yml",
            ".githooks/pre-commit",
        ],
        paths
    );
    assert_eq!(true, managed[2]["executable"]);
    assert_eq!(".githooks", document["hooks_path"]);

    let workflow = String::from_utf8(render_repository_workflow()).unwrap();
    assert!(workflow.contains("permissions:\n  contents: read"));
    assert!(workflow.contains(&format!(
        "python -m pip install \"datajig=={}\"",
        env!("CARGO_PKG_VERSION")
    )));
    assert!(workflow.contains("datajig repository-check --root . --ci"));
    assert!(workflow.contains("actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09"));
    assert!(workflow.contains("actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"));

    let hook = String::from_utf8(render_repository_hook()).unwrap();
    assert!(hook.starts_with("#!/bin/sh\n"));
    assert!(hook.contains("git rev-parse --show-toplevel"));
    assert!(hook.contains("datajig repository-check --root"));

    let contract = document["agent_contract_id"].as_str().unwrap();
    assert!(contract.starts_with("contract_"));
    assert!(
        managed.iter().all(|file| {
            file["content_id"]
                .as_str()
                .is_some_and(|value| value.starts_with("managed_"))
        }),
        "every managed asset must be content-addressed"
    );
}

#[test]
fn repository_install_publishes_exact_assets_lock_last_and_activates_hook() {
    let root = temporary_git_repository("first install");
    let artifact = install_repository(
        &root,
        &[],
        RepositoryComponents {
            hook: true,
            github_actions: true,
        },
    )
    .unwrap();

    assert_eq!("installed", artifact.decision);
    assert_eq!(3, artifact.managed_files);
    assert!(artifact.hook_active);
    assert!(artifact.repository_integration_id.starts_with("repo_"));
    assert_eq!(
        render_agent_skill().as_bytes(),
        fs::read(root.join(".agents/skills/datajig/SKILL.md"))
            .unwrap()
            .as_slice()
    );
    assert_eq!(
        render_repository_workflow(),
        fs::read(root.join(".github/workflows/datajig.yml")).unwrap()
    );
    assert_eq!(
        render_repository_hook(),
        fs::read(root.join(".githooks/pre-commit")).unwrap()
    );
    assert!(root.join(".datajig-repository.json").is_file());
    #[cfg(unix)]
    assert_ne!(
        0,
        fs::metadata(root.join(".githooks/pre-commit"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111
    );
    #[cfg(unix)]
    {
        assert_eq!(
            1,
            fs::metadata(root.join(".githooks/pre-commit"))
                .unwrap()
                .nlink()
        );
        assert_eq!(
            1,
            fs::metadata(root.join(".datajig-repository.json"))
                .unwrap()
                .nlink()
        );
    }
    assert_eq!(
        ".githooks",
        git_output(&root, &["config", "--local", "--get", "core.hooksPath"])
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn repository_install_is_idempotent_in_paths_with_spaces_and_unicode() {
    let root = temporary_git_repository("space 研究");
    let components = RepositoryComponents {
        hook: true,
        github_actions: true,
    };
    let first = install_repository(&root, &[], components).unwrap();
    let lock_before = fs::read(root.join(".datajig-repository.json")).unwrap();
    let second = install_repository(&root, &[], components).unwrap();

    assert_eq!(
        first.repository_integration_id,
        second.repository_integration_id
    );
    assert_eq!("unchanged", second.decision);
    assert_eq!(
        lock_before,
        fs::read(root.join(".datajig-repository.json")).unwrap()
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn repository_install_refuses_unknown_or_modified_managed_targets() {
    for (path, payload) in [
        (
            ".agents/skills/datajig/SKILL.md",
            b"unknown skill".as_slice(),
        ),
        (
            ".github/workflows/datajig.yml",
            b"unknown workflow".as_slice(),
        ),
    ] {
        let root = temporary_git_repository("unknown-target");
        let target = root.join(path);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, payload).unwrap();
        assert!(
            install_repository(
                &root,
                &[],
                RepositoryComponents {
                    hook: false,
                    github_actions: true,
                },
            )
            .is_err()
        );
        assert!(!root.join(".datajig-repository.json").exists());
        assert_eq!(payload, fs::read(target).unwrap());
        let _ = fs::remove_dir_all(root);
    }

    let root = temporary_git_repository("modified-managed-target");
    install_repository(
        &root,
        &[],
        RepositoryComponents {
            hook: false,
            github_actions: false,
        },
    )
    .unwrap();
    let skill = root.join(".agents/skills/datajig/SKILL.md");
    fs::write(&skill, b"locally modified").unwrap();
    assert!(
        install_repository(
            &root,
            &[],
            RepositoryComponents {
                hook: false,
                github_actions: false,
            },
        )
        .is_err()
    );
    assert_eq!(b"locally modified", fs::read(skill).unwrap().as_slice());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn repository_install_recovers_exact_hook_bytes_left_without_executable_mode() {
    let root = temporary_git_repository("hook-mode-recovery");
    let skill = root.join(".agents/skills/datajig/SKILL.md");
    fs::create_dir_all(skill.parent().unwrap()).unwrap();
    fs::write(&skill, render_agent_skill()).unwrap();
    let hook = root.join(".githooks/pre-commit");
    fs::create_dir_all(hook.parent().unwrap()).unwrap();
    fs::write(&hook, render_repository_hook()).unwrap();
    let mut permissions = fs::metadata(&hook).unwrap().permissions();
    permissions.set_mode(0o644);
    fs::set_permissions(&hook, permissions).unwrap();

    install_repository(
        &root,
        &[],
        RepositoryComponents {
            hook: true,
            github_actions: false,
        },
    )
    .unwrap();
    assert_ne!(0, fs::metadata(&hook).unwrap().permissions().mode() & 0o111);
    assert!(check_repository(&root, false).is_ok());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn repository_install_rejects_existing_inactive_reserved_paths_before_writing() {
    for path in [".github/workflows/datajig.yml", ".githooks/pre-commit"] {
        let root = temporary_git_repository("inactive-reserved");
        let target = root.join(path);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, b"user-owned").unwrap();

        assert!(
            install_repository(
                &root,
                &[],
                RepositoryComponents {
                    hook: false,
                    github_actions: false,
                },
            )
            .is_err()
        );
        assert!(!root.join(".agents").exists());
        assert!(!root.join(".datajig-repository.json").exists());
        assert_eq!(b"user-owned", fs::read(target).unwrap().as_slice());
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn repository_install_refuses_conflicting_hooks_path_without_mutation() {
    let root = temporary_git_repository("hooks-conflict");
    run_git(
        &root,
        &["config", "--local", "core.hooksPath", "custom-hooks"],
    );

    assert!(
        install_repository(
            &root,
            &[],
            RepositoryComponents {
                hook: true,
                github_actions: true,
            },
        )
        .is_err()
    );
    assert_eq!(
        "custom-hooks",
        git_output(&root, &["config", "--local", "--get", "core.hooksPath"])
    );
    assert!(!root.join(".agents").exists());
    assert!(!root.join(".datajig-repository.json").exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn repository_install_supports_verified_monotonic_upgrade_and_partial_recovery() {
    let root = temporary_git_repository("upgrade");
    let initial = install_repository(
        &root,
        &[],
        RepositoryComponents {
            hook: false,
            github_actions: false,
        },
    )
    .unwrap();
    let old_lock = fs::read(root.join(".datajig-repository.json")).unwrap();

    // Simulate an interrupted upgrade: desired assets reached disk, old lock remained.
    let workflow = root.join(".github/workflows/datajig.yml");
    fs::create_dir_all(workflow.parent().unwrap()).unwrap();
    fs::write(&workflow, render_repository_workflow()).unwrap();
    let upgraded = install_repository(
        &root,
        &[PathBuf::from(".datajig-training")],
        RepositoryComponents {
            hook: false,
            github_actions: true,
        },
    )
    .unwrap();

    assert_eq!("updated", upgraded.decision);
    assert_ne!(
        initial.repository_integration_id,
        upgraded.repository_integration_id
    );
    assert_ne!(
        old_lock,
        fs::read(root.join(".datajig-repository.json")).unwrap()
    );
    assert_eq!(render_repository_workflow(), fs::read(workflow).unwrap());

    // Components and state bindings cannot silently be removed.
    assert!(
        install_repository(
            &root,
            &[],
            RepositoryComponents {
                hook: false,
                github_actions: false,
            },
        )
        .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn repository_install_cli_emits_json_and_recovers_after_each_commit_boundary() {
    for failpoint in ["after_assets", "after_hook_config"] {
        let root = temporary_git_repository(failpoint);
        let failed = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
            .args(["repository-install", "--root"])
            .arg(&root)
            .env("DATAJIG_TEST_REPOSITORY_FAILPOINT", failpoint)
            .output()
            .unwrap();
        assert!(
            !failed.status.success(),
            "failpoint must stop the subprocess"
        );
        assert!(
            !root.join(".datajig-repository.json").exists(),
            "lock must remain the final commit point"
        );

        let recovered = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
            .args(["repository-install", "--root"])
            .arg(&root)
            .output()
            .unwrap();
        assert!(
            recovered.status.success(),
            "recovery failed: {}",
            String::from_utf8_lossy(&recovered.stderr)
        );
        assert!(recovered.stderr.is_empty());
        let document: serde_json::Value = serde_json::from_slice(&recovered.stdout).unwrap();
        assert_eq!("repository_integration_installed", document["kind"]);
        assert_eq!("installed", document["decision"]);
        assert_eq!("installed", document["artifact"]["decision"]);
        assert!(
            document["artifact"]["repository_integration_id"]
                .as_str()
                .unwrap()
                .starts_with("repo_")
        );
        assert_eq!(
            ".githooks",
            git_output(&root, &["config", "--local", "--get", "core.hooksPath"])
        );
        let checked = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
            .args(["repository-check", "--root"])
            .arg(&root)
            .output()
            .unwrap();
        assert!(checked.status.success());
        let checked: serde_json::Value = serde_json::from_slice(&checked.stdout).unwrap();
        assert_eq!("repository_integration_checked", checked["kind"]);
        assert_eq!("ready", checked["decision"]);
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(unix)]
#[test]
fn repository_install_rejects_hard_linked_managed_targets() {
    let root = temporary_git_repository("hard-link-target");
    let outside = unique_temporary_path("outside-skill");
    fs::write(&outside, render_agent_skill()).unwrap();
    let target = root.join(".agents/skills/datajig/SKILL.md");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::hard_link(&outside, &target).unwrap();

    assert!(
        install_repository(
            &root,
            &[],
            RepositoryComponents {
                hook: false,
                github_actions: false,
            },
        )
        .is_err()
    );
    assert!(!root.join(".datajig-repository.json").exists());
    assert_eq!(render_agent_skill().as_bytes(), fs::read(&outside).unwrap());
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_file(outside);
}

#[test]
fn repository_check_detects_asset_hook_and_contract_drift_without_writes() {
    let root = temporary_git_repository("check-drift");
    install_repository(
        &root,
        &[],
        RepositoryComponents {
            hook: true,
            github_actions: true,
        },
    )
    .unwrap();

    let ready = check_repository(&root, false).unwrap();
    assert_eq!("ready", ready.decision);
    assert_eq!(3, ready.managed_files);
    let hook = root.join(".githooks/pre-commit");
    let mut permissions = fs::metadata(&hook).unwrap().permissions();
    permissions.set_mode(0o644);
    fs::set_permissions(&hook, permissions).unwrap();
    assert!(check_repository(&root, true).is_err());
    let mut permissions = fs::metadata(&hook).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&hook, permissions).unwrap();
    run_git(&root, &["config", "--local", "--unset", "core.hooksPath"]);
    assert!(check_repository(&root, false).is_err());
    assert!(check_repository(&root, true).is_ok());

    fs::write(root.join(".agents/skills/datajig/SKILL.md"), b"tampered").unwrap();
    assert!(check_repository(&root, true).is_err());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn repository_check_requires_every_bound_workspace_to_be_clean_and_inside_root() {
    let root = temporary_git_repository("workspace-readiness");
    let dataset = root.join("data/train.jsonl");
    fs::create_dir_all(dataset.parent().unwrap()).unwrap();
    fs::write(&dataset, b"{\"id\":\"one\",\"text\":\"ready\"}\n").unwrap();
    let state = root.join(".datajig-training");
    initialize_jsonl_workspace(&dataset, &state, "id").unwrap();
    install_repository(
        &root,
        &[PathBuf::from(".datajig-training")],
        RepositoryComponents {
            hook: false,
            github_actions: false,
        },
    )
    .unwrap();

    let ready = check_repository(&root, false).unwrap();
    assert_eq!(1, ready.states.len());
    assert!(ready.states[0].clean);
    fs::write(&dataset, b"{\"id\":\"one\",\"text\":\"changed\"}\n").unwrap();
    assert!(check_repository(&root, false).is_err());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn repository_check_rejects_inactive_assets_and_redirected_or_replaced_states() {
    let root = temporary_git_repository("inactive-asset");
    install_repository(
        &root,
        &[],
        RepositoryComponents {
            hook: false,
            github_actions: false,
        },
    )
    .unwrap();
    let workflow = root.join(".github/workflows/datajig.yml");
    fs::create_dir_all(workflow.parent().unwrap()).unwrap();
    fs::write(&workflow, render_repository_workflow()).unwrap();
    assert!(check_repository(&root, false).is_err());
    let _ = fs::remove_dir_all(root);

    let root = temporary_git_repository("state-redirect");
    for name in ["alpha", "zeta"] {
        let dataset = root.join(format!("data/{name}.jsonl"));
        fs::create_dir_all(dataset.parent().unwrap()).unwrap();
        fs::write(&dataset, format!("{{\"id\":\"{name}\"}}\n")).unwrap();
        initialize_jsonl_workspace(&dataset, &root.join(format!(".{name}-state")), "id").unwrap();
    }
    install_repository(
        &root,
        &[PathBuf::from(".zeta-state"), PathBuf::from(".alpha-state")],
        RepositoryComponents {
            hook: false,
            github_actions: false,
        },
    )
    .unwrap();
    let ready = check_repository(&root, false).unwrap();
    assert_eq!(".alpha-state", ready.states[0].state);
    assert_eq!(".zeta-state", ready.states[1].state);

    let state = root.join(".alpha-state");
    let saved = root.join("alpha-state-saved");
    fs::rename(&state, &saved).unwrap();
    std::os::unix::fs::symlink(&saved, &state).unwrap();
    assert!(check_repository(&root, false).is_err());
    fs::remove_file(&state).unwrap();
    fs::rename(&saved, &state).unwrap();
    assert!(check_repository(&root, false).is_ok());

    let replaced = root.join("alpha-state-replaced");
    fs::rename(&state, &replaced).unwrap();
    fs::create_dir(&state).unwrap();
    assert!(check_repository(&root, false).is_err());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn linked_worktree_installs_while_symlinked_or_bare_roots_fail() {
    let root = temporary_git_repository("worktree-source");
    run_git(&root, &["config", "user.name", "DataJig Test"]);
    run_git(&root, &["config", "user.email", "liukj7@gmail.com"]);
    run_git(&root, &["commit", "--allow-empty", "-m", "initial"]);
    let linked = unique_temporary_path("linked worktree");
    let linked_text = linked.to_str().unwrap();
    run_git(
        &root,
        &["worktree", "add", "-b", "linked-test", linked_text],
    );

    install_repository(
        &linked,
        &[],
        RepositoryComponents {
            hook: false,
            github_actions: false,
        },
    )
    .unwrap();

    let symlink = unique_temporary_path("root-symlink");
    std::os::unix::fs::symlink(&root, &symlink).unwrap();
    assert!(
        install_repository(
            &symlink,
            &[],
            RepositoryComponents {
                hook: false,
                github_actions: false,
            },
        )
        .is_err()
    );

    let bare = unique_temporary_path("bare.git");
    let status = Command::new("git")
        .args(["init", "--bare"])
        .arg(&bare)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(
        install_repository(
            &bare,
            &[],
            RepositoryComponents {
                hook: false,
                github_actions: false,
            },
        )
        .is_err()
    );

    let _ = fs::remove_file(symlink);
    let _ = fs::remove_dir_all(linked);
    let _ = fs::remove_dir_all(bare);
    let _ = fs::remove_dir_all(root);
}

fn temporary_git_repository(label: &str) -> PathBuf {
    let root = unique_temporary_path(label);
    fs::create_dir(&root).unwrap();
    run_git(&root, &["init"]);
    root
}

fn run_git(root: &std::path::Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_output(root: &std::path::Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn unique_temporary_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "datajig-repository-{}-{nonce}-{label}",
        std::process::id()
    ))
}
