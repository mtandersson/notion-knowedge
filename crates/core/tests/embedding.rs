use notion_knowledge_core::embedding::{
    EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider, embed_batch,
};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

struct Fake {
    metadata: EmbeddingMetadata,
    result: Option<Result<Vec<Vec<f32>>, EmbeddingError>>,
}
impl Fake {
    fn new() -> Self {
        Self {
            metadata: metadata("1"),
            result: None,
        }
    }
}
fn metadata(version: &str) -> EmbeddingMetadata {
    EmbeddingMetadata::new("fake".into(), "byte-count".into(), version.into(), 2).unwrap()
}
impl EmbeddingProvider for Fake {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            self.result.clone().unwrap_or_else(|| {
                Ok(inputs
                    .iter()
                    .map(|s| vec![s.len() as f32, s.bytes().map(u32::from).sum::<u32>() as f32])
                    .collect())
            })
        })
    }
}
fn run<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::noop();
    match std::pin::pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(waker))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("deterministic fake must be immediately ready"),
    }
}

#[test]
fn batches_preserve_order_duplicates_and_deterministic_vectors_through_port() {
    let fake = Fake::new();
    let inputs = vec!["abc".into(), "z".into(), "abc".into(), "".into()];
    let expected = vec![vec![3., 294.], vec![1., 122.], vec![3., 294.], vec![0., 0.]];
    assert_eq!(run(embed_batch(&fake, &inputs)).unwrap(), expected);
    assert_eq!(run(embed_batch(&fake, &inputs)).unwrap(), expected);
    let failing = Fake {
        result: Some(Err(EmbeddingError::Unavailable)),
        ..Fake::new()
    };
    assert!(run(embed_batch(&failing, &[])).unwrap().is_empty());
}

#[test]
fn malformed_provider_outputs_are_permanent_errors_before_indexing() {
    for result in [
        vec![],
        vec![vec![1., 2.], vec![3., 4.]],
        vec![vec![1.]],
        vec![vec![f32::NAN, 2.]],
        vec![vec![1., f32::INFINITY]],
    ] {
        let fake = Fake {
            result: Some(Ok(result)),
            ..Fake::new()
        };
        assert_eq!(
            run(embed_batch(&fake, &["secret text".into()])),
            Err(EmbeddingError::InvalidOutput)
        );
    }
    assert!(!EmbeddingError::InvalidOutput.is_retryable());
}

#[test]
fn provider_failures_retain_retry_class_without_input_payloads() {
    for error in [
        EmbeddingError::Unavailable,
        EmbeddingError::ResourceExhausted,
        EmbeddingError::InvalidInput,
        EmbeddingError::UnsupportedModel,
    ] {
        let fake = Fake {
            result: Some(Err(error)),
            ..Fake::new()
        };
        assert_eq!(run(embed_batch(&fake, &["secret text".into()])), Err(error));
        assert_eq!(
            error.is_retryable(),
            matches!(
                error,
                EmbeddingError::Unavailable | EmbeddingError::ResourceExhausted
            )
        );
        assert!(!error.to_string().contains("secret text"));
    }
}

#[test]
fn persisted_metadata_requires_supported_valid_identity_and_explicit_rebuild() {
    let original = metadata("1");
    assert_eq!(original.provider_id(), "fake");
    assert_eq!(original.model_id(), "byte-count");
    assert_eq!(original.model_version(), "1");
    let wire = serde_json::to_value(&original).unwrap();
    assert_eq!(wire["schema_version"], "1");
    let restored: EmbeddingMetadata = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(original.ensure_compatible(&restored), Ok(()));
    for (field, replacement) in [
        ("schema_version", serde_json::json!("2")),
        ("dimension", serde_json::json!(0)),
        ("model_id", serde_json::json!(" ")),
    ] {
        let mut invalid = wire.clone();
        invalid[field] = replacement;
        assert!(serde_json::from_value::<EmbeddingMetadata>(invalid).is_err());
    }
    for (field, replacement) in [
        ("provider_id", serde_json::json!("other")),
        ("model_id", serde_json::json!("other")),
        ("model_version", serde_json::json!("2")),
        ("dimension", serde_json::json!(3)),
    ] {
        let mut changed = wire.clone();
        changed[field] = replacement;
        let changed: EmbeddingMetadata = serde_json::from_value(changed).unwrap();
        assert_eq!(
            original.ensure_compatible(&changed),
            Err(EmbeddingError::IncompatibleIndex)
        );
    }
}
