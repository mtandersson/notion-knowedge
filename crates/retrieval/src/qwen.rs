//! Offline, bounded Qwen3-Embedding-0.6B implementation of the core batch port.
use candle_core::{DType, Device};
use candle_nn::VarBuilder;
use fastembed::{Qwen3Config, Qwen3Model, Qwen3TextEmbedding};
use notion_knowledge_core::embedding::{
    EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider,
};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;

pub const REVISION: &str = "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3";
pub const DIMENSION: usize = 1024;
const ASSETS: [(&str, &str); 3] = [
    (
        "config.json",
        "b5bf1f51fc45be473a54718cef92448d90a1be001bf9b9a44b8c7f10a19feaa9",
    ),
    (
        "tokenizer.json",
        "def76fb086971c7867b829c23a26261e38d9d74e02139253b38aeb9df8b4b50a",
    ),
    (
        "model.safetensors",
        "0437e45c94563b09e13cb7a64478fc406947a93cb34a7e05870fc8dcd48e23fd",
    ),
];
/// Explicit device selection; this build supports CPU only. Unsupported device
/// requests fail instead of silently falling back or changing vector precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QwenDevice {
    Cpu,
    Cuda(usize),
    Metal(usize),
}

#[derive(Debug, Clone)]
pub struct QwenConfig {
    pub assets: PathBuf,
    /// Maximum texts per runtime forward pass (1..=8).
    pub batch_size: usize,
    pub device: QwenDevice,
    /// Maximum tokens per text (1..=4096); excess is rejected, never truncated.
    pub max_tokens: usize,
}
impl QwenConfig {
    pub fn cpu(assets: impl Into<PathBuf>) -> Self {
        Self {
            assets: assets.into(),
            batch_size: 1,
            device: QwenDevice::Cpu,
            max_tokens: 512,
        }
    }
    fn validate(&self) -> Result<(), EmbeddingError> {
        if self.device != QwenDevice::Cpu {
            return Err(EmbeddingError::UnsupportedModel);
        }
        if !(1..=8).contains(&self.batch_size) || !(1..=4096).contains(&self.max_tokens) {
            return Err(EmbeddingError::InvalidInput);
        }
        Ok(())
    }
}
/// Metadata records weights, tokenizer, precision, pooling and normalization.
/// Batch/input limits do not change accepted input vectors. Query instruction
/// formatting is owned by the caller; this provider embeds exact supplied text.
pub fn metadata() -> EmbeddingMetadata {
    EmbeddingMetadata::new("local-fastembed-candle-cpu-f32".into(),
        "Qwen/Qwen3-Embedding-0.6B".into(),
        format!("{REVISION}:fastembed7.1.0:candle0.11.0:tokenizers0.23.2:last-token:left-padding:l2:exact-text:untruncated-v1"),
        DIMENSION).expect("constant valid model identity")
}
trait BatchRuntime: Send {
    fn token_count(&self, input: &str) -> Result<usize, EmbeddingError>;
    fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}
struct Runtime {
    model: Qwen3TextEmbedding,
    tokenizer: tokenizers::Tokenizer,
}
impl BatchRuntime for Runtime {
    fn token_count(&self, input: &str) -> Result<usize, EmbeddingError> {
        self.tokenizer
            .encode(input, true)
            .map(|e| e.len())
            .map_err(|_| EmbeddingError::InvalidInput)
    }
    fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        self.model
            .embed(inputs)
            .map_err(|_| EmbeddingError::Unavailable)
    }
}
/// One active batch per instance. Concurrent calls fail with ResourceExhausted
/// rather than creating an unbounded blocking queue. Drop cancels awaiting the
/// result; already-started CPU work continues holding its permit until done.
pub struct QwenProvider {
    runtime: Arc<Mutex<Box<dyn BatchRuntime>>>,
    admission: Arc<Semaphore>,
    config: QwenConfig,
    metadata: EmbeddingMetadata,
}
fn checked_asset(path: &Path, expected: &str) -> Result<Vec<u8>, EmbeddingError> {
    let bytes = std::fs::read(path).map_err(|_| EmbeddingError::Unavailable)?;
    if format!("{:x}", Sha256::digest(&bytes)) != expected {
        return Err(EmbeddingError::UnsupportedModel);
    }
    Ok(bytes)
}
impl QwenProvider {
    /// Reads only caller-provided files; never downloads, reads credentials or
    /// initializes an ONNX runtime. Requires a Tokio runtime for blocking work.
    pub async fn load(config: QwenConfig) -> Result<Self, EmbeddingError> {
        config.validate()?;
        let assets = config.assets.clone();
        let runtime = tokio::task::spawn_blocking(move || {
            // Verify precisely the owned bytes subsequently parsed/loaded.
            let cfg_bytes = checked_asset(&assets.join(ASSETS[0].0), ASSETS[0].1)?;
            let tokenizer_bytes = checked_asset(&assets.join(ASSETS[1].0), ASSETS[1].1)?;
            let weights = checked_asset(&assets.join(ASSETS[2].0), ASSETS[2].1)?;
            let cfg: Qwen3Config =
                serde_json::from_slice(&cfg_bytes).map_err(|_| EmbeddingError::UnsupportedModel)?;
            if cfg.hidden_size != DIMENSION {
                return Err(EmbeddingError::UnsupportedModel);
            }
            let vb = VarBuilder::from_buffered_safetensors(weights, DType::F32, &Device::Cpu)
                .map_err(|_| EmbeddingError::Unavailable)?;
            let model = Qwen3Model::new(cfg, vb).map_err(|_| EmbeddingError::Unavailable)?;
            let mut tokenizer = tokenizers::Tokenizer::from_bytes(&tokenizer_bytes)
                .map_err(|_| EmbeddingError::UnsupportedModel)?;
            tokenizer
                .with_truncation(None)
                .map_err(|_| EmbeddingError::UnsupportedModel)?;
            tokenizer.with_padding(None);
            let mut padded = tokenizer.clone();
            padded.with_padding(Some(tokenizers::PaddingParams {
                strategy: tokenizers::PaddingStrategy::BatchLongest,
                direction: tokenizers::PaddingDirection::Left,
                ..Default::default()
            }));
            Ok(Runtime {
                model: Qwen3TextEmbedding::new(model, padded),
                tokenizer,
            })
        })
        .await
        .map_err(|_| EmbeddingError::Unavailable)??;
        Ok(Self {
            runtime: Arc::new(Mutex::new(Box::new(runtime))),
            admission: Arc::new(Semaphore::new(1)),
            config,
            metadata: metadata(),
        })
    }
    /// Create a sidecar once. Never overwrite an existing identity silently.
    /// Storage adapters must also persist/compare this record with index state.
    pub fn persist_metadata(&self, path: &Path) -> Result<(), EmbeddingError> {
        persist_metadata(&self.metadata, path)
    }
}
fn persist_metadata(identity: &EmbeddingMetadata, path: &Path) -> Result<(), EmbeddingError> {
    use std::io::Write;
    let bytes = serde_json::to_vec_pretty(identity).map_err(|_| EmbeddingError::InvalidMetadata)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| EmbeddingError::Unavailable)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| EmbeddingError::Unavailable)
}
fn validate_inputs(inputs: &[String]) -> Result<(), EmbeddingError> {
    // Bound cloning/tokenization before admission. Caller can split large jobs.
    if inputs.len() > 32
        || inputs
            .iter()
            .any(|s| s.trim().is_empty() || s.len() > 32768)
        || inputs.iter().map(String::len).sum::<usize>() > 262144
    {
        return Err(EmbeddingError::InvalidInput);
    }
    Ok(())
}
impl EmbeddingProvider for QwenProvider {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            validate_inputs(inputs)?;
            if inputs.is_empty() {
                return Ok(Vec::new());
            }
            let permit = self
                .admission
                .clone()
                .try_acquire_owned()
                .map_err(|_| EmbeddingError::ResourceExhausted)?;
            let inputs = inputs.to_vec();
            let runtime = self.runtime.clone();
            let config = self.config.clone();
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let runtime = runtime.lock().map_err(|_| EmbeddingError::Unavailable)?;
                // Validate all inputs before any forward pass, without truncation.
                let lengths: Vec<_> = inputs
                    .iter()
                    .map(|s| runtime.token_count(s))
                    .collect::<Result<_, _>>()?;
                if lengths.iter().any(|&n| n == 0 || n > config.max_tokens) {
                    return Err(EmbeddingError::InvalidInput);
                }
                if lengths
                    .chunks(config.batch_size)
                    .any(|tokens| tokens.iter().max().copied().unwrap_or(0) * tokens.len() > 4096)
                {
                    return Err(EmbeddingError::ResourceExhausted);
                }
                let mut output = Vec::with_capacity(inputs.len());
                for batch in inputs.chunks(config.batch_size) {
                    let vectors = runtime.embed(batch)?;
                    if vectors.len() != batch.len()
                        || vectors
                            .iter()
                            .any(|v| v.len() != DIMENSION || v.iter().any(|x| !x.is_finite()))
                    {
                        return Err(EmbeddingError::InvalidOutput);
                    }
                    output.extend(vectors);
                }
                Ok(output)
            })
            .await
            .map_err(|_| EmbeddingError::Unavailable)?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_limits_and_unsupported_devices_fail_explicitly() {
        for n in [0, 9, usize::MAX] {
            let mut c = QwenConfig::cpu("unused");
            c.batch_size = n;
            assert_eq!(c.validate(), Err(EmbeddingError::InvalidInput));
        }
        for n in [0, 4097] {
            let mut c = QwenConfig::cpu("unused");
            c.max_tokens = n;
            assert_eq!(c.validate(), Err(EmbeddingError::InvalidInput));
        }
        for d in [QwenDevice::Cuda(0), QwenDevice::Metal(0)] {
            let mut c = QwenConfig::cpu("unused");
            c.device = d;
            assert_eq!(c.validate(), Err(EmbeddingError::UnsupportedModel));
        }
    }
    #[test]
    fn input_budget_rejects_empty_and_excessive_text_without_echoing() {
        for text in ["".to_string(), " \n".to_string(), "å".repeat(32769)] {
            assert_eq!(validate_inputs(&[text]), Err(EmbeddingError::InvalidInput));
        }
        assert!(
            validate_inputs(&[
                "Svensk text".into(),
                "English text".into(),
                "Svensk text".into()
            ])
            .is_ok()
        );
        assert_eq!(
            validate_inputs(&vec!["x".into(); 33]),
            Err(EmbeddingError::InvalidInput)
        );
    }
    #[tokio::test]
    async fn missing_assets_return_sanitized_failure_without_network() {
        assert!(matches!(
            QwenProvider::load(QwenConfig::cpu("/nonexistent/nk-model")).await,
            Err(EmbeddingError::Unavailable)
        ));
    }
    #[test]
    fn incompatible_asset_is_rejected_before_runtime_initialization() {
        let p = std::env::temp_dir().join(format!("nk39-invalid-{}", std::process::id()));
        std::fs::write(&p, b"untrusted contents").unwrap();
        assert_eq!(
            checked_asset(&p, ASSETS[0].1),
            Err(EmbeddingError::UnsupportedModel)
        );
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn persisted_identity_round_trips_and_cannot_overwrite_existing_record() {
        let p = std::env::temp_dir().join(format!("nk39-identity-{}", std::process::id()));
        let identity = metadata();
        persist_metadata(&identity, &p).unwrap();
        let loaded: EmbeddingMetadata =
            serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
        identity.ensure_compatible(&loaded).unwrap();
        assert_eq!(
            persist_metadata(&identity, &p),
            Err(EmbeddingError::Unavailable)
        );
        let changed = EmbeddingMetadata::new(
            identity.provider_id().into(),
            identity.model_id().into(),
            "changed-weights".into(),
            DIMENSION,
        )
        .unwrap();
        assert_eq!(
            loaded.ensure_compatible(&changed),
            Err(EmbeddingError::IncompatibleIndex)
        );
        std::fs::remove_file(p).unwrap();
    }
    struct Fake {
        calls: Arc<Mutex<Vec<Vec<String>>>>,
        malformed: bool,
    }
    impl BatchRuntime for Fake {
        fn token_count(&self, input: &str) -> Result<usize, EmbeddingError> {
            Ok(input.parse().unwrap_or(1))
        }
        fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            self.calls.lock().unwrap().push(inputs.to_vec());
            if self.malformed {
                return Ok(vec![vec![f32::NAN]]);
            }
            Ok(inputs
                .iter()
                .map(|s| vec![s.len() as f32; DIMENSION])
                .collect())
        }
    }
    fn fake(config: QwenConfig, malformed: bool) -> (QwenProvider, Arc<Mutex<Vec<Vec<String>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            QwenProvider {
                runtime: Arc::new(Mutex::new(Box::new(Fake {
                    calls: calls.clone(),
                    malformed,
                }))),
                admission: Arc::new(Semaphore::new(1)),
                config,
                metadata: metadata(),
            },
            calls,
        )
    }
    #[tokio::test]
    async fn batches_preserve_input_order_and_duplicates() {
        let mut c = QwenConfig::cpu("unused");
        c.batch_size = 2;
        let (p, calls) = fake(c, false);
        let texts = ["Svenska", "English text", "Svenska"].map(str::to_string);
        let output = notion_knowledge_core::embedding::embed_batch(&p, &texts)
            .await
            .unwrap();
        assert_eq!(output.len(), 3);
        assert_eq!(output[0], output[2]);
        assert_ne!(output[0], output[1]);
        assert_eq!(
            *calls.lock().unwrap(),
            vec![texts[..2].to_vec(), texts[2..].to_vec()]
        );
        assert_eq!(p.embed_batch(&[]).await.unwrap(), Vec::<Vec<f32>>::new());
        assert_eq!(calls.lock().unwrap().len(), 2);
    }
    #[tokio::test]
    async fn overlong_or_oversized_padded_batch_never_runs_inference() {
        let mut c = QwenConfig::cpu("unused");
        c.batch_size = 2;
        c.max_tokens = 4096;
        let (p, calls) = fake(c, false);
        assert_eq!(
            p.embed_batch(&["1".into(), "4097".into()]).await,
            Err(EmbeddingError::InvalidInput)
        );
        assert_eq!(
            p.embed_batch(&["1".into(), "3000".into()]).await,
            Err(EmbeddingError::ResourceExhausted)
        );
        assert!(calls.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn concurrent_admission_is_bounded_and_invalid_vectors_are_rejected() {
        let (p, _) = fake(QwenConfig::cpu("unused"), true);
        let permit = p.admission.clone().try_acquire_owned().unwrap();
        assert_eq!(
            p.embed_batch(&["text".into()]).await,
            Err(EmbeddingError::ResourceExhausted)
        );
        drop(permit);
        assert_eq!(
            p.embed_batch(&["text".into()]).await,
            Err(EmbeddingError::InvalidOutput)
        );
    }
    struct Blocking {
        started: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl BatchRuntime for Blocking {
        fn token_count(&self, _: &str) -> Result<usize, EmbeddingError> {
            Ok(1)
        }
        fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            self.started
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            self.release.lock().unwrap().recv().unwrap();
            Ok(vec![vec![0.0; DIMENSION]; inputs.len()])
        }
    }
    #[tokio::test]
    async fn canceled_caller_retains_admission_until_blocking_work_finishes() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let p = Arc::new(QwenProvider {
            runtime: Arc::new(Mutex::new(Box::new(Blocking {
                started: Mutex::new(Some(started_tx)),
                release: Mutex::new(release_rx),
            }))),
            admission: Arc::new(Semaphore::new(1)),
            config: QwenConfig::cpu("unused"),
            metadata: metadata(),
        });
        let running = p.clone();
        let task = tokio::spawn(async move { running.embed_batch(&["text".into()]).await });
        started_rx.await.unwrap();
        task.abort();
        assert_eq!(
            p.embed_batch(&["second".into()]).await,
            Err(EmbeddingError::ResourceExhausted)
        );
        release_tx.send(()).unwrap();
        let permit = p.admission.clone().acquire_owned().await.unwrap();
        drop(permit);
        assert_eq!(p.admission.available_permits(), 1);
    }
}
