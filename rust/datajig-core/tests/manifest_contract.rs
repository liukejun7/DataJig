use datajig_core::{ManifestEntry, SnapshotManifest, snapshot_id};

fn entry(path: &str, size: u64, digest_byte: char) -> ManifestEntry {
    ManifestEntry::new(path.to_owned(), size, digest_byte.to_string().repeat(64))
        .expect("fixture entry should be valid")
}

#[test]
fn snapshot_identity_is_order_independent_and_content_sensitive() {
    let first = entry("a/data.jsonl", 7, 'a');
    let second = entry("b/image.png", 11, 'b');

    let forward = snapshot_id(&[first.clone(), second.clone()]).expect("identity should build");
    let reversed = snapshot_id(&[second.clone(), first.clone()]).expect("identity should build");
    let changed =
        snapshot_id(&[first, entry("b/image.png", 12, 'b')]).expect("identity should build");

    assert_eq!(forward, reversed);
    assert_ne!(forward, changed);
    assert_eq!(69, forward.len());
    assert!(forward.starts_with("snap_"));
}

#[test]
fn manifest_round_trip_preserves_canonical_order_identity_and_summary() {
    let manifest = SnapshotManifest::create(vec![
        entry("z-last.txt", 2, 'f'),
        entry("a-first.txt", 3, '0'),
    ])
    .expect("manifest should build");
    let encoded = manifest.to_json().expect("manifest should serialize");
    let decoded = SnapshotManifest::from_json(&encoded).expect("manifest should parse");

    assert_eq!(manifest, decoded);
    assert_eq!("a-first.txt", decoded.entries()[0].path());
    assert_eq!(2, decoded.summary().total_files);
    assert_eq!(5, decoded.summary().total_bytes);
}

#[test]
fn manifest_parser_rejects_unknown_fields_and_identity_tampering() {
    let manifest =
        SnapshotManifest::create(vec![entry("data.jsonl", 4, 'c')]).expect("manifest should build");
    let encoded = manifest.to_json().expect("manifest should serialize");
    let mut document: serde_json::Value =
        serde_json::from_str(&encoded).expect("fixture should contain JSON");

    document["unexpected"] = serde_json::json!(true);
    let unknown_field = SnapshotManifest::from_json(&document.to_string())
        .expect_err("unknown fields must fail closed");
    assert!(format!("{unknown_field:#}").contains("unknown field"));

    document
        .as_object_mut()
        .expect("manifest should be an object")
        .remove("unexpected");
    document["snapshot_id"] = serde_json::json!(format!("snap_{}", "0".repeat(64)));
    let tampered = SnapshotManifest::from_json(&document.to_string())
        .expect_err("a forged identity must fail closed");
    assert!(
        tampered
            .to_string()
            .contains("snapshot_id does not match manifest entries")
    );
}

#[test]
fn manifest_entries_reject_noncanonical_paths() {
    for path in ["", "/absolute", "a//b", "a/../b", "./a", "a\\b"] {
        assert!(
            ManifestEntry::new(path.to_owned(), 1, "0".repeat(64)).is_err(),
            "path should be rejected: {path:?}"
        );
    }
}
