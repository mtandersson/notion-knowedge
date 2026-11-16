//! The one place that turns embedding configuration into a provider.
//!
//! Every runtime builds its provider here, so a different backend changes this
//! function only. A provider whose vector identity does not match the expected
//! index identity is rejected before any caller can embed with it.
use std::{future::Future, path::PathBuf, sync::Arc};

use notion_knowledge_core::embedding::{EmbeddingError, EmbeddingMetadata, EmbeddingProvider};

/// Configured embedding backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedderConfig {
    /// Offline Qwen3 on CPU from pinned local assets.
    LocalQwen { assets: PathBuf },
}

/// Load the configured provider. When `expected` is given, a provider with a
/// different vector identity fails with `IncompatibleIndex` and is dropped unused.
pub async fn load_configured_embedder(
    config: &EmbedderConfig,
    expected: Option<&EmbeddingMetadata>,
) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
    load_with(config, expected, load_backend).await
}

async fn load_backend(
    config: &EmbedderConfig,
) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
    match config {
        #[cfg(feature = "local-qwen")]
        EmbedderConfig::LocalQwen { assets } => Ok(Arc::new(
            crate::qwen::QwenProvider::load(crate::qwen::QwenConfig::cpu(assets)).await?,
        )),
        #[cfg(not(feature = "local-qwen"))]
        EmbedderConfig::LocalQwen { .. } => Err(EmbeddingError::UnsupportedModel),
    }
}

async fn load_with<'c, F, Fut>(
    config: &'c EmbedderConfig,
    expected: Option<&EmbeddingMetadata>,
    backend: F,
) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError>
where
    F: FnOnce(&'c EmbedderConfig) -> Fut,
    Fut: Future<Output = Result<Arc<dyn EmbeddingProvider>, EmbeddingError>>,
{
    let provider = backend(config).await?;
    if let Some(expected) = expected {
        expected.ensure_compatible(provider.metadata())?;
    }
    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::embedding::EmbeddingFuture;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Stub {
        metadata: EmbeddingMetadata,
        calls: Arc<AtomicUsize>,
    }
    impl EmbeddingProvider for Stub {
        fn metadata(&self) -> &EmbeddingMetadata {
            &self.metadata
        }
        fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let dimension = self.metadata.dimension();
            Box::pin(async move { Ok(vec![vec![0.0; dimension]; inputs.len()]) })
        }
    }

    fn identity(model_version: &str) -> EmbeddingMetadata {
        EmbeddingMetadata::new("stub".into(), "stub-model".into(), model_version.into(), 4).unwrap()
    }

    fn config() -> EmbedderConfig {
        EmbedderConfig::LocalQwen {
            assets: PathBuf::from("/unused"),
        }
    }

    async fn load_stub(
        provided: EmbeddingMetadata,
        expected: Option<&EmbeddingMetadata>,
        calls: Arc<AtomicUsize>,
    ) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
        let config = config();
        load_with(&config, expected, |_| async move {
            Ok(Arc::new(Stub {
                metadata: provided,
                calls,
            }) as Arc<dyn EmbeddingProvider>)
        })
        .await
    }

    #[tokio::test]
    async fn factory_rejects_identity_mismatch_before_use() {
        let calls = Arc::new(AtomicUsize::new(0));
        let result = load_stub(identity("v2"), Some(&identity("v1")), calls.clone()).await;
        assert_eq!(result.err(), Some(EmbeddingError::IncompatibleIndex));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn factory_returns_provider_with_matching_or_unchecked_identity() {
        let calls = Arc::new(AtomicUsize::new(0));
        for expected in [Some(identity("v1")), None] {
            let provider = load_stub(identity("v1"), expected.as_ref(), calls.clone())
                .await
                .expect("compatible provider");
            assert_eq!(provider.metadata(), &identity("v1"));
        }
    }

    #[tokio::test]
    async fn backend_failure_is_returned_unchanged() {
        let config = config();
        let result = load_with(&config, None, |_| async {
            Err(EmbeddingError::UnsupportedModel)
        })
        .await;
        assert_eq!(result.err(), Some(EmbeddingError::UnsupportedModel));
    }
}
