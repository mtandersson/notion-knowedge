//! Descriptive compiled identities, never executable compatibility authority.
use super::*;

const PROTOCOL: &str = "offline-lexical-runtime-v1;policy-independent-physical-fencing";

/// Self-description of an explicitly trusted runtime and selected lexical mode.
/// Parsing this metadata cannot establish that its executable passes the native
/// fencing probe. Only that independent verification may authorize rollback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDescriptor {
    version: u32,
    protocol: String,
    authority_implementation_sha256: String,
    storage: StorageIdentity,
    plan: Option<LexicalPlanIdentity>,
}
impl RuntimeDescriptor {
    /// Describe this build's actual implementation. No old asset or retained
    /// source identity is accepted as an argument to relabel the current code.
    /// A dictionary must already have passed this build's immutable loader.
    pub fn current(dictionary: Option<&Dictionary>) -> Result<Self, ChunkTableError> {
        let plan = dictionary.map(LexicalPlanIdentity::verified).transpose()?;
        let mut storage = StorageIdentity::current();
        if plan.is_none() {
            storage.profile = FtsIndexConfig::default();
        }
        Ok(Self {
            version: 1,
            protocol: PROTOCOL.into(),
            authority_implementation_sha256: digest(
                concat!(
                    include_str!("../../commit.rs"),
                    include_str!("../../commit/offline.rs"),
                    include_str!("../../sync_state.rs"),
                    include_str!("../guarded.rs"),
                    include_str!("runtime.rs"),
                )
                .as_bytes(),
            ),
            storage,
            plan,
        })
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ChunkTableError> {
        let bytes = serde_json::to_vec(self).map_err(|_| incompatible())?;
        if bytes.len() as u64 > MAX_BYTES {
            return Err(incompatible());
        }
        Ok(bytes)
    }
    /// Parse bounded canonical metadata. This checks its shape and supported
    /// protocol, not the truth of an executable's claims or source compatibility.
    pub fn parse_metadata(bytes: &[u8]) -> Result<Self, ChunkTableError> {
        if bytes.len() as u64 > MAX_BYTES {
            return Err(incompatible());
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| incompatible())?;
        if value.version != 1
            || value.protocol != PROTOCOL
            || !valid_digest(&value.authority_implementation_sha256)
            || !valid_digest(&value.storage.implementation_sha256)
            || value.canonical_bytes()? != bytes
        {
            return Err(incompatible());
        }
        // Old implementation digests may be described, but schema/profile
        // combinations and every lexical identity must remain well-formed.
        let mut storage = StorageIdentity::current();
        storage.implementation_sha256 = value.storage.implementation_sha256.clone();
        if let Some(plan) = &value.plan {
            if ![
                &plan.asset_sha256,
                &plan.importer_sha256,
                &plan.executor_sha256,
                &plan.lock_sha256,
            ]
            .into_iter()
            .all(|v| valid_digest(v))
                || plan.embedding_text_contract
                    != "canonical-indexed-chunk-v1;embedding-input=chunk.text;unchanged-vector-space"
            {
                return Err(incompatible());
            }
        } else {
            storage.profile = FtsIndexConfig::default();
        }
        if value.storage != storage {
            return Err(incompatible());
        }
        Ok(value)
    }
    /// Strict metadata comparison for a bound prior source. True is necessary,
    /// never sufficient: it does not verify a native index, executable or asset.
    pub fn describes_bound_source(&self, binding: &LexicalIndexBinding) -> bool {
        self.storage == binding.storage && self.plan.as_ref() == Some(&binding.plan)
    }
    /// Legacy absence is not itself evidence about the previously used build.
    /// A caller must retain a trusted executable and verify the actual source.
    pub fn is_legacy(&self) -> bool {
        self.plan.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mutate(value: &RuntimeDescriptor, field: &str, new: serde_json::Value) -> Vec<u8> {
        let mut json = serde_json::to_value(value).unwrap();
        json[field] = new;
        // Re-serialize the fixed struct so ordinary object map ordering does not
        // obscure the specific identity/protocol boundary under test.
        serde_json::to_vec(&serde_json::from_value::<RuntimeDescriptor>(json).unwrap()).unwrap()
    }
    #[test]
    fn runtime_metadata_keeps_legacy_and_verified_expansion_semantics_distinct() {
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        let legacy = RuntimeDescriptor::current(None).unwrap();
        let expanded = RuntimeDescriptor::current(Some(&dictionary)).unwrap();
        assert!(legacy.is_legacy());
        assert!(!expanded.is_legacy());
        assert_ne!(legacy.storage.profile, expanded.storage.profile);
        assert_eq!(
            expanded.plan,
            Some(LexicalPlanIdentity::verified(&dictionary).unwrap())
        );
        for descriptor in [legacy, expanded] {
            assert_eq!(
                RuntimeDescriptor::parse_metadata(&descriptor.canonical_bytes().unwrap()).unwrap(),
                descriptor
            );
        }
    }
    #[test]
    fn bound_source_description_requires_the_complete_prior_storage_and_plan() {
        use notion_knowledge_core::reconciliation::ReconciliationScope;
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("cinnamon", "kanel")]);
        let descriptor = RuntimeDescriptor::current(Some(&dictionary)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let scope =
            ReconciliationScope::new(vec!["root".into()], vec![], "policy", "source-policy")
                .unwrap();
        let authority =
            CommitBinding::new("chunks", "workspace", &scope, "physical-generation", 9).unwrap();
        let embedding =
            EmbeddingMetadata::new("fixture".into(), "model".into(), "v1".into(), 3).unwrap();
        let binding = LexicalIndexBinding::for_candidate(
            directory.path(),
            embedding,
            &authority,
            LexicalPlanIdentity::verified(&dictionary).unwrap(),
            &"a".repeat(64),
        )
        .unwrap();
        assert!(descriptor.describes_bound_source(&binding));
        assert!(
            !RuntimeDescriptor::current(None)
                .unwrap()
                .describes_bound_source(&binding)
        );
        let mut old_storage = binding.clone();
        old_storage.storage.implementation_sha256 = "b".repeat(64);
        assert!(!descriptor.describes_bound_source(&old_storage));
        let mut old_plan = binding;
        old_plan.plan.asset_sha256 = "c".repeat(64);
        assert!(!descriptor.describes_bound_source(&old_plan));
    }
    #[test]
    fn runtime_metadata_refuses_unknown_protocols_and_malformed_or_excess_data() {
        let value = RuntimeDescriptor::current(None).unwrap();
        for bytes in [
            mutate(&value, "protocol", "coupled-generation-runtime".into()),
            mutate(&value, "version", 2.into()),
            mutate(
                &value,
                "authority_implementation_sha256",
                "unverified".into(),
            ),
            vec![b' '; MAX_BYTES as usize + 1],
            {
                let mut b = value.canonical_bytes().unwrap();
                b.push(b'\n');
                b
            },
        ] {
            assert!(RuntimeDescriptor::parse_metadata(&bytes).is_err());
        }
        let mut json = serde_json::to_value(value).unwrap();
        json["assume_compatible"] = true.into();
        assert!(RuntimeDescriptor::parse_metadata(&serde_json::to_vec(&json).unwrap()).is_err());
    }
}
