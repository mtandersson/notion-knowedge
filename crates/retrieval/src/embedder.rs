//! The one place that turns embedding configuration into a provider.
//!
//! Every server runtime builds its provider here, so a different backend
//! changes this module only. A caller that already knows the expected index
//! identity can pass it, and a mismatched provider is then rejected before it
//! is returned. Callers that pass `None` rely on the index runtime's own
//! `ensure_compatible` check against persisted metadata.
use std::{future::Future, path::PathBuf, sync::Arc};

use notion_knowledge_core::embedding::{EmbeddingError, EmbeddingMetadata, EmbeddingProvider};

/// Configured embedding backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedderConfig {
    /// Offline Qwen3 on CPU from pinned local assets.
    LocalQwen { assets: PathBuf },
}

/// Construct the configured vector identity without opening model assets.
#[cfg(feature = "local-qwen")]
pub fn deferred_configured_embedder(config: EmbedderConfig) -> Arc<dyn EmbeddingProvider> {
    Arc::new(DeferredEmbedder {
        config,
        metadata: crate::qwen::metadata(),
        state: tokio::sync::Mutex::new(DeferredState::Idle),
    })
}

#[cfg(feature = "local-qwen")]
struct DeferredEmbedder {
    config: EmbedderConfig,
    metadata: EmbeddingMetadata,
    state: tokio::sync::Mutex<DeferredState>,
}

#[cfg(feature = "local-qwen")]
enum DeferredState {
    Idle,
    Loading(tokio::task::JoinHandle<Result<Arc<dyn EmbeddingProvider>, EmbeddingError>>),
    Ready(Arc<dyn EmbeddingProvider>),
}

#[cfg(feature = "local-qwen")]
impl DeferredState {
    /// Keep the owned load in this state if its query or worker loses interest.
    /// A later caller joins that load, and shutdown drains it.
    async fn finish(&mut self) -> Result<Option<Arc<dyn EmbeddingProvider>>, EmbeddingError> {
        match self {
            Self::Idle => Ok(None),
            Self::Ready(provider) => Ok(Some(provider.clone())),
            Self::Loading(task) => {
                let result = task.await.unwrap_or(Err(EmbeddingError::Unavailable));
                *self = match &result {
                    Ok(provider) => Self::Ready(provider.clone()),
                    Err(_) => Self::Idle,
                };
                result.map(Some)
            }
        }
    }
}

#[cfg(feature = "local-qwen")]
impl DeferredEmbedder {
    async fn provider(&self) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
        let mut state = self.state.lock().await;
        if matches!(*state, DeferredState::Idle) {
            let config = self.config.clone();
            let metadata = self.metadata.clone();
            *state = DeferredState::Loading(tokio::spawn(async move {
                load_configured_embedder(&config, Some(&metadata)).await
            }));
        }
        state.finish().await?.ok_or(EmbeddingError::Unavailable)
    }
}

#[cfg(feature = "local-qwen")]
impl EmbeddingProvider for DeferredEmbedder {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }

    fn wait_idle(&self) -> notion_knowledge_core::embedding::EmbeddingIdleFuture<'_> {
        Box::pin(async move {
            let provider = self.state.lock().await.finish().await.ok().flatten();
            if let Some(provider) = provider {
                provider.wait_idle().await?;
            }
            Ok(())
        })
    }

    fn embed_batch<'a>(
        &'a self,
        inputs: &'a [String],
    ) -> notion_knowledge_core::embedding::EmbeddingFuture<'a> {
        Box::pin(async move { self.provider().await?.embed_batch(inputs).await })
    }

    fn partition_input<'a>(
        &'a self,
        input: &'a str,
        boundaries: Vec<usize>,
    ) -> notion_knowledge_core::embedding::EmbeddingPartitionFuture<'a> {
        Box::pin(async move {
            self.provider()
                .await?
                .partition_input(input, boundaries)
                .await
        })
    }

    fn embedding_windows<'a>(
        &'a self,
        input: &'a str,
        boundaries: Vec<usize>,
    ) -> notion_knowledge_core::embedding::EmbeddingWindowsFuture<'a> {
        Box::pin(async move {
            self.provider()
                .await?
                .embedding_windows(input, boundaries)
                .await
        })
    }
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
    async fn factory_returns_unused_provider_with_matching_or_unchecked_identity() {
        for expected in [Some(identity("v1")), None] {
            let calls = Arc::new(AtomicUsize::new(0));
            let provider = load_stub(identity("v1"), expected.as_ref(), calls.clone())
                .await
                .expect("compatible provider");
            assert_eq!(provider.metadata(), &identity("v1"));
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn backend_failure_is_returned_without_identity_check() {
        let config = config();
        let result = load_with(&config, Some(&identity("v1")), |_| async {
            Err(EmbeddingError::Unavailable)
        })
        .await;
        assert_eq!(result.err(), Some(EmbeddingError::Unavailable));
    }

    #[cfg(feature = "local-qwen")]
    #[tokio::test]
    async fn model_identity_and_drain_require_no_assets() {
        let temp = tempfile::tempdir().unwrap();
        let provider = deferred_configured_embedder(EmbedderConfig::LocalQwen {
            assets: temp.path().join("missing"),
        });
        assert_eq!(provider.metadata(), &crate::qwen::metadata());
        assert!(provider.wait_idle().await.is_ok());
        for _ in 0..2 {
            assert_eq!(
                provider.embed_batch(&["query".into()]).await,
                Err(EmbeddingError::Unavailable)
            );
            assert!(provider.wait_idle().await.is_ok());
        }
    }

    #[cfg(not(feature = "local-qwen"))]
    #[tokio::test]
    async fn local_qwen_is_unsupported_without_the_feature() {
        let result = load_configured_embedder(&config(), None).await;
        assert_eq!(result.err(), Some(EmbeddingError::UnsupportedModel));
    }
}
