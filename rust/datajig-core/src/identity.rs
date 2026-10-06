pub const IDENTITY_NAMESPACE: &str = "datajig-v1";
pub const ARTIFACT_NAMESPACE: &str = "datajig";

pub fn blake3_content_id(prefix: &str, domain: &[u8], payload: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(payload);
    format!("{prefix}_{}", hasher.finalize().to_hex())
}
