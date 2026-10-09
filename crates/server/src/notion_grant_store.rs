//! Versioned AES-256-GCM Notion grant store. The encryption key is supplied
//! out of band in a separate 32-byte, private, read-only file. Neither the
//! provider token nor the key is put in logs, indices, MCP or config Debug.
//! This is storage only: no HTTP login or MCP bearer authorization is enabled.
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use ring::{
    aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};

use crate::notion_oauth_callback::{NotionGrant, NotionOwnerPolicy};

const MAGIC: &[u8; 8] = b"NKGRANT1";
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const MAX_FILE: usize = 32 * 1024;
const AAD: &[u8] = b"notion-knowledge/notion-oauth-grant/v1";
const SCHEMA: u32 = 1;

/// These variants do not include paths, OS errors or sensitive record data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    Configuration,
    Unavailable,
    Corrupt,
    IdentityMismatch,
    Expired,
}
impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Configuration => "Notion grant store configuration invalid",
            Self::Unavailable => "Notion grant store unavailable",
            Self::Corrupt => "Notion grant store invalid or unreadable",
            Self::IdentityMismatch => "Notion grant identity binding rejected",
            Self::Expired => "Notion grant expired; reauthorization required",
        })
    }
}
impl std::error::Error for StoreError {}

/// Metadata is encrypted together with the tokens. This is internal only,
/// never serialized as an API response or sent to the client.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema: u32,
    grant_id: String,
    epoch: u64,
    notion_client_id: String,
    workspace_id: String,
    owner_user_id: String,
    bot_id: String,
    issued_at_unix: u64,
    expires_at_unix: Option<u64>,
    access_token: String,
    refresh_token: Option<String>,
}
pub struct StoredGrant {
    record: Record,
}
impl std::fmt::Debug for StoredGrant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoredGrant([REDACTED])")
    }
}
impl StoredGrant {
    pub fn grant_id(&self) -> &str { &self.record.grant_id }
    pub fn epoch(&self) -> u64 { self.record.epoch }
    pub fn workspace_id(&self) -> &str { &self.record.workspace_id }
    pub fn owner_user_id(&self) -> &str { &self.record.owner_user_id }
    pub fn issued_at_unix(&self) -> u64 { self.record.issued_at_unix }
    pub fn expires_at_unix(&self) -> Option<u64> { self.record.expires_at_unix }
    // Only server-side refresh / Notion adapters may obtain token material.
    pub(crate) fn access_token(&self) -> &str { &self.record.access_token }
    pub(crate) fn refresh_token(&self) -> Option<&str> { self.record.refresh_token.as_deref() }
}

/// Encrypts ALL records at rest with authenticated metadata and distinct
/// random nonces. One in-process writer is serialized; multi-process ownership
/// and revocation fencing are work for #124/#127.
pub struct GrantStore {
    file: PathBuf,
    key: LessSafeKey,
    policy: NotionOwnerPolicy,
    notion_client_id: String,
    guard: Mutex<()>,
}
impl std::fmt::Debug for GrantStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GrantStore([REDACTED])")
    }
}
impl GrantStore {
    /// A missing data file means "no approved grant", not first-login
    /// enrollment. Any unreadable, corrupt or foreign existing state fails.
    pub fn open(key_file: &Path, state_file: &Path, client_id: &str, policy: NotionOwnerPolicy) -> Result<Self, StoreError> {
        if !valid_path(key_file) || !valid_path(state_file) || key_file == state_file
            || client_id.is_empty() || client_id.len() > 128 {
            return Err(StoreError::Configuration);
        }
        let directory = state_file.parent().ok_or(StoreError::Configuration)?;
        require_directory(directory)?;
        let key_metadata = fs::symlink_metadata(key_file).map_err(|_| StoreError::Configuration)?;
        if !key_metadata.file_type().is_file()
            || key_metadata.permissions().mode() & 0o077 != 0 {
            return Err(StoreError::Configuration);
        }
        let mut key_bytes = [0u8; 32];
        let mut key_source = File::open(key_file).map_err(|_| StoreError::Configuration)?;
        key_source.read_exact(&mut key_bytes).map_err(|_| StoreError::Configuration)?;
        let mut extra = [0u8; 1];
        if key_source.read(&mut extra).map_err(|_| StoreError::Configuration)? != 0 {
            return Err(StoreError::Configuration);
        }
        let key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, &key_bytes).map_err(|_| StoreError::Configuration)?
        );
        // No retained plaintext key buffer after key schedule initialization.
        key_bytes.fill(0);
        let store = Self {
            file: state_file.to_owned(),
            key,
            policy,
            notion_client_id: client_id.to_owned(),
            guard: Mutex::new(()),
        };
        store.read_record()?; // startup checks integrity + owner binding
        Ok(store)
    }

    /// Load only an unexpired, still correctly pinned grant. An unknown
    /// provider expiry is represented explicitly as None, never synthesized.
    pub fn load(&self) -> Result<Option<StoredGrant>, StoreError> {
        let _guard = self.guard.lock().map_err(|_| StoreError::Unavailable)?;
        let record = self.read_record()?;
        if let Some(ref value) = record {
            if value.expires_at_unix.is_some_and(|expiry| expiry <= now_unix()?) {
                return Err(StoreError::Expired);
            }
        }
        Ok(record.map(|record| StoredGrant { record }))
    }

    /// Persist only a typed, already-verified Notion callback grant that
    /// matches the immutable operator policy. This does NOT approve MCP login.
    /// Every replacement receives a fresh grant ID and increasing epoch,
    /// invalidating earlier session bindings once #127 enforces them.
    pub fn save(&self, grant: &NotionGrant) -> Result<StoredGrant, StoreError> {
        if !self.policy.matches(grant.workspace_id(), grant.owner_user_id()) {
            return Err(StoreError::IdentityMismatch);
        }
        let _guard = self.guard.lock().map_err(|_| StoreError::Unavailable)?;
        let old = self.read_record()?;
        let epoch = match old {
            Some(record) => record.epoch.checked_add(1).ok_or(StoreError::Corrupt)?,
            None => 1,
        };
        let now = now_unix()?;
        let expires = grant.expires_in().map(|seconds| {
            now.checked_add(seconds).ok_or(StoreError::Corrupt)
        }).transpose()?;
        let mut grant_id = [0u8; 16];
        SystemRandom::new().fill(&mut grant_id).map_err(|_| StoreError::Unavailable)?;
        let record = Record {
            schema: SCHEMA,
            grant_id: hex(&grant_id),
            epoch,
            notion_client_id: self.notion_client_id.clone(),
            workspace_id: grant.workspace_id().to_owned(),
            owner_user_id: grant.owner_user_id().to_owned(),
            bot_id: grant.bot_id().to_owned(),
            issued_at_unix: now,
            expires_at_unix: expires,
            access_token: grant.access_token().to_owned(),
            refresh_token: grant.refresh_token().map(str::to_owned),
        };
        let raw = serde_json::to_vec(&record).map_err(|_| StoreError::Unavailable)?;
        if raw.len() + MAGIC.len() + NONCE_LEN + TAG_LEN > MAX_FILE {
            return Err(StoreError::Unavailable);
        }
        let mut nonce_bytes = [0u8; NONCE_LEN];
        SystemRandom::new().fill(&mut nonce_bytes).map_err(|_| StoreError::Unavailable)?;
        let mut ciphertext = raw;
        self.key
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce_bytes), Aad::from(AAD), &mut ciphertext)
            .map_err(|_| StoreError::Unavailable)?;
        let directory = self.file.parent().ok_or(StoreError::Configuration)?;
        require_directory(directory)?;
        let mut tmp = tempfile::NamedTempFile::new_in(directory).map_err(|_| StoreError::Unavailable)?;
        tmp.as_file_mut().set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| StoreError::Unavailable)?;
        tmp.write_all(MAGIC).and_then(|_| tmp.write_all(&nonce_bytes))
            .and_then(|_| tmp.write_all(&ciphertext)).map_err(|_| StoreError::Unavailable)?;
        tmp.as_file_mut().sync_all().map_err(|_| StoreError::Unavailable)?;
        // Do not follow links or overwrite suspicious existing state.
        self.validate_existing_file()?;
        tmp.persist(&self.file).map_err(|_| StoreError::Unavailable)?;
        File::open(directory).and_then(|d| d.sync_all()).map_err(|_| StoreError::Unavailable)?;
        Ok(StoredGrant { record })
    }

    fn validate_existing_file(&self) -> Result<(), StoreError> {
        match fs::symlink_metadata(&self.file) {
            Ok(metadata) if metadata.file_type().is_file()
                && metadata.permissions().mode() & 0o077 == 0 => Ok(()),
            Ok(_) => Err(StoreError::Corrupt),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(StoreError::Unavailable),
        }
    }

    fn read_record(&self) -> Result<Option<Record>, StoreError> {
        self.validate_existing_file()?;
        let file = match File::open(&self.file) {
            Ok(value) => value,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(StoreError::Unavailable),
        };
        let mut raw = Vec::new();
        file.take((MAX_FILE + 1) as u64).read_to_end(&mut raw).map_err(|_| StoreError::Unavailable)?;
        if raw.len() <= MAGIC.len() + NONCE_LEN + TAG_LEN || raw.len() > MAX_FILE
            || &raw[..MAGIC.len()] != MAGIC {
            return Err(StoreError::Corrupt);
        }
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&raw[MAGIC.len()..MAGIC.len() + NONCE_LEN]);
        let mut payload = raw[MAGIC.len() + NONCE_LEN..].to_vec();
        let plaintext = self.key.open_in_place(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(AAD),
            &mut payload,
        ).map_err(|_| StoreError::Corrupt)?;
        let record: Record = serde_json::from_slice(plaintext).map_err(|_| StoreError::Corrupt)?;
        if record.schema != SCHEMA
            || record.epoch == 0
            || record.grant_id.len() != 32 || !record.grant_id.bytes().all(|b| b.is_ascii_hexdigit())
            || record.bot_id.is_empty()
            || record.issued_at_unix == 0
            || record.expires_at_unix.is_some_and(|at| at <= record.issued_at_unix)
            || record.access_token.is_empty()
            || record.refresh_token.as_ref().is_some_and(String::is_empty)
        { return Err(StoreError::Corrupt); }
        if record.notion_client_id != self.notion_client_id
            || !self.policy.matches(&record.workspace_id, &record.owner_user_id) {
            return Err(StoreError::IdentityMismatch);
        }
        Ok(Some(record))
    }
}
fn now_unix() -> Result<u64, StoreError> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::Unavailable)?.as_secs())
}
fn valid_path(path: &Path) -> bool {
    path.is_absolute() && path.file_name().is_some()
        && !path.components().any(|c| matches!(c, std::path::Component::ParentDir))
}
fn require_directory(path: &Path) -> Result<(), StoreError> {
    let m = fs::symlink_metadata(path).map_err(|_| StoreError::Configuration)?;
    if !m.file_type().is_dir() || m.permissions().mode() & 0o022 != 0 {
        return Err(StoreError::Configuration);
    }
    Ok(())
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        result.push(DIGITS[(b >> 4) as usize] as char);
        result.push(DIGITS[(b & 15) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (TempDir, PathBuf, PathBuf, NotionOwnerPolicy) {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("key");
        fs::write(&key, [42u8; 32]).unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o600)).unwrap();
        let file = dir.path().join("grant.bin");
        let policy = NotionOwnerPolicy::new("workspace-a", "user-a").unwrap();
        (dir, key, file, policy)
    }

    #[test]
    fn encrypted_round_trip_is_durable_and_secrets_never_on_disk() {
        let (_dir,key,path,policy) = setup();
        let store = GrantStore::open(&key, &path, "client-a", policy.clone()).unwrap();
        assert!(store.load().unwrap().is_none());
        let grant = NotionGrant::fixture("workspace-a", "user-a", Some(3600));
        let first = store.save(&grant).unwrap();
        assert_eq!(first.epoch(), 1);
        assert_eq!(first.workspace_id(), "workspace-a");
        assert!(first.expires_at_unix().unwrap() > first.issued_at_unix());
        assert!(format!("{first:?}").contains("REDACTED"));
        let bytes = fs::read(&path).unwrap();
        assert!(bytes.starts_with(MAGIC));
        for secret in ["test-access-secret","test-refresh-secret","workspace-a","user-a"] {
            assert!(!bytes.windows(secret.len()).any(|w| w == secret.as_bytes()));
        }
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        let reloaded = GrantStore::open(&key, &path, "client-a", policy).unwrap().load().unwrap().unwrap();
        assert_eq!(reloaded.grant_id(), first.grant_id());
        assert_eq!(reloaded.access_token(), "test-access-secret");
        assert_eq!(reloaded.refresh_token(), Some("test-refresh-secret"));
        let second = store.save(&grant).unwrap();
        assert_eq!(second.epoch(), 2);
        assert_ne!(second.grant_id(), first.grant_id());
        assert_ne!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn wrong_key_tampering_schema_and_foreign_owner_fail_closed() {
        let (_dir,key,path,policy) = setup();
        let store = GrantStore::open(&key,&path,"client-a",policy.clone()).unwrap();
        store.save(&NotionGrant::fixture("workspace-a","user-a",None)).unwrap();
        let mut raw = fs::read(&path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0x80;
        fs::write(&path,&raw).unwrap();
        assert!(matches!(store.load(),Err(StoreError::Corrupt)));
        assert!(matches!(GrantStore::open(&key,&path,"client-a",policy.clone()),Err(StoreError::Corrupt)));
        // Restore a valid envelope, then test a different key and identity.
        fs::remove_file(&path).unwrap();
        store.save(&NotionGrant::fixture("workspace-a","user-a",None)).unwrap();
        fs::write(&key,[7u8;32]).unwrap();
        assert!(matches!(GrantStore::open(&key,&path,"client-a",policy.clone()),Err(StoreError::Corrupt)));
        fs::write(&key,[42u8;32]).unwrap();
        assert!(matches!(GrantStore::open(&key,&path,"different-client",policy.clone()),Err(StoreError::IdentityMismatch)));
        let foreign = NotionOwnerPolicy::new("foreign","user-a").unwrap();
        assert!(matches!(GrantStore::open(&key,&path,"client-a",foreign),Err(StoreError::IdentityMismatch)));
        assert!(matches!(store.save(&NotionGrant::fixture("foreign","user-a",None)),Err(StoreError::IdentityMismatch)));
    }

    #[test]
    fn symlinks_permissive_modes_and_missing_key_are_rejected() {
        let (dir,key,path,policy) = setup();
        fs::set_permissions(&key,fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(GrantStore::open(&key,&path,"client-a",policy.clone()),Err(StoreError::Configuration)));
        fs::set_permissions(&key,fs::Permissions::from_mode(0o600)).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&key,&link).unwrap();
        assert!(matches!(GrantStore::open(&link,&path,"client-a",policy.clone()),Err(StoreError::Configuration)));
        fs::write(&path,b"plaintext-grant").unwrap();
        assert!(matches!(GrantStore::open(&key,&path,"client-a",policy.clone()),Err(StoreError::Corrupt)));
        fs::remove_file(&path).unwrap();
        let store = GrantStore::open(&key,&path,"client-a",policy.clone()).unwrap();
        let outside = dir.path().join("outside");
        fs::write(&outside,b"not-a-grant").unwrap();
        std::os::unix::fs::symlink(&outside,&path).unwrap();
        assert!(matches!(store.load(),Err(StoreError::Corrupt)));
        assert!(matches!(store.save(&NotionGrant::fixture("workspace-a","user-a",None)),Err(StoreError::Corrupt)));
        fs::remove_file(&path).unwrap();
        fs::remove_file(&key).unwrap();
        assert!(matches!(GrantStore::open(&key,&path,"client-a",policy),Err(StoreError::Configuration)));
    }
}
