//! Trusted default-off local dictionary configuration; never a tool input.
use crate::config::ConfigError;
#[cfg(all(unix, feature = "webhook-worker"))]
use std::sync::{Arc, OnceLock};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

#[derive(Clone)]
pub struct Settings {
    bundle: PathBuf,
    sha256: String,
    max_requests: u32,
    #[cfg(all(unix, feature = "webhook-worker"))]
    dictionary: Arc<
        OnceLock<
            Result<Arc<notion_knowledge_retrieval::lexical_dictionary::Dictionary>, &'static str>,
        >,
    >,
}

impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LexicalSettings")
            .field("max_requests", &self.max_requests)
            .finish_non_exhaustive()
    }
}
impl Settings {
    pub fn bundle(&self) -> &Path {
        &self.bundle
    }
    pub fn expected_sha256(&self) -> &str {
        &self.sha256
    }
    pub fn max_requests(&self) -> u32 {
        self.max_requests
    }

    pub(crate) fn parse(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Option<Self>, ConfigError> {
        let error = |setting, reason| ConfigError { setting, reason };
        match lookup("NK_LEXICAL_EXPANSION").as_deref() {
            None => return Ok(None),
            Some(value) if value == "false" => return Ok(None),
            Some(value) if value == "true" => {}
            _ => return Err(error("NK_LEXICAL_EXPANSION", "must be true or false")),
        }
        if !cfg!(all(unix, feature = "webhook-worker")) {
            return Err(error(
                "NK_LEXICAL_EXPANSION",
                "requires Unix webhook-worker feature",
            ));
        }
        let raw = lookup("NK_LEXICAL_ASSET_BUNDLE")
            .and_then(|v| v.into_string().ok())
            .ok_or_else(|| {
                error(
                    "NK_LEXICAL_ASSET_BUNDLE",
                    "requires an absolute local bundle path",
                )
            })?;
        let bundle = PathBuf::from(&raw);
        if !bundle.is_absolute() || raw.len() > 4096 || raw.chars().any(char::is_control) {
            return Err(error(
                "NK_LEXICAL_ASSET_BUNDLE",
                "requires an absolute local bundle path",
            ));
        }
        let sha256 = lookup("NK_LEXICAL_ASSET_SHA256")
            .and_then(|v| v.into_string().ok())
            .filter(|s| {
                s.len() == 64
                    && s.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            .ok_or_else(|| {
                error(
                    "NK_LEXICAL_ASSET_SHA256",
                    "requires a lowercase SHA256 digest",
                )
            })?;
        let max_requests = lookup("NK_LEXICAL_MAX_REQUESTS")
            .map(|v| v.into_string().ok().and_then(|s| s.parse::<u32>().ok()))
            .unwrap_or(Some(1))
            .filter(|n| (1..=16).contains(n))
            .ok_or_else(|| error("NK_LEXICAL_MAX_REQUESTS", "must be an integer from 1 to 16"))?;
        Ok(Some(Self {
            bundle,
            sha256,
            max_requests,
            #[cfg(all(unix, feature = "webhook-worker"))]
            dictionary: Arc::new(OnceLock::new()),
        }))
    }

    #[cfg(all(unix, feature = "webhook-worker"))]
    /// Trusted dependency seam for callers already holding a verified bundle.
    /// This is not an environment-selectable loader or a request parameter.
    pub fn with_verified_dictionary(
        mut self,
        dictionary: Arc<notion_knowledge_retrieval::lexical_dictionary::Dictionary>,
    ) -> Result<Self, &'static str> {
        if dictionary.manifest_digest() != self.sha256 {
            return Err("lexical dictionary and configured digest differ");
        }
        self.dictionary = Arc::new(OnceLock::from(Ok(dictionary)));
        Ok(self)
    }

    #[cfg(all(unix, feature = "webhook-worker"))]
    pub(crate) fn load_plan(
        &self,
    ) -> Result<notion_knowledge_retrieval::chunks::lexical_binding::LexicalCachePlan, &'static str>
    {
        use notion_knowledge_retrieval::{
            chunks::lexical_binding::LexicalCachePlan, lexical_dictionary::Dictionary,
        };
        let dictionary = self
            .dictionary
            .get_or_init(|| {
                Dictionary::load(&self.bundle, &self.sha256)
                    .map(Arc::new)
                    .map_err(|_| "lexical assets unavailable or incompatible")
            })
            .clone()?;
        LexicalCachePlan::verified(dictionary)
            .map_err(|_| "lexical plan unavailable or incompatible")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expansion_defaults_off_and_rejects_implicit_enablement() {
        assert!(Settings::parse(&mut |_| None).unwrap().is_none());
        assert!(
            Settings::parse(&mut |key| (key == "NK_LEXICAL_EXPANSION").then(|| "false".into()))
                .unwrap()
                .is_none()
        );
        let error =
            Settings::parse(&mut |key| (key == "NK_LEXICAL_EXPANSION").then(|| "yes".into()))
                .unwrap_err();
        assert_eq!(error.setting, "NK_LEXICAL_EXPANSION");
    }
    #[cfg(all(unix, feature = "webhook-worker"))]
    #[test]
    fn enabled_settings_are_bounded_and_do_not_echo_private_paths() {
        let parse = |path: &str, digest: &str, count: &str| {
            Settings::parse(&mut |key| match key {
                "NK_LEXICAL_EXPANSION" => Some("true".into()),
                "NK_LEXICAL_ASSET_BUNDLE" => Some(path.into()),
                "NK_LEXICAL_ASSET_SHA256" => Some(digest.into()),
                "NK_LEXICAL_MAX_REQUESTS" => Some(count.into()),
                _ => None,
            })
        };
        let digest = "a".repeat(64);
        let settings = parse("/tmp/synthetic-private-bundle", &digest, "1")
            .unwrap()
            .unwrap();
        assert_eq!(
            settings.bundle(),
            Path::new("/tmp/synthetic-private-bundle")
        );
        assert_eq!(settings.max_requests(), 1);
        assert!(!format!("{settings:?}").contains("synthetic-private-bundle"));
        for (path, hash, count) in [
            ("synthetic-private-relative", digest.as_str(), "1"),
            ("/tmp/synthetic-private-bundle", "BAD", "1"),
            ("/tmp/synthetic-private-bundle", digest.as_str(), "0"),
            ("/tmp/synthetic-private-bundle", digest.as_str(), "17"),
        ] {
            let error = parse(path, hash, count).unwrap_err();
            assert!(!error.to_string().contains("synthetic-private"));
        }
    }
}
