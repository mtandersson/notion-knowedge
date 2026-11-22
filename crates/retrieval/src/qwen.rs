//! Offline, bounded Qwen3-Embedding-0.6B implementation of the core batch port.
use candle_core::{DType, Device};
use candle_nn::VarBuilder;
use fastembed::{Qwen3Config, Qwen3Model, Qwen3TextEmbedding};
use notion_knowledge_core::embedding::{
    EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingPartitionFuture,
    EmbeddingProvider, partition_at_boundaries,
};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Weak},
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
            max_tokens: 1024,
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
        format!("{REVISION}:fastembed7.1.0:candle0.11.1:tokenizers0.23.2:last-token:left-padding:l2:exact-text:untruncated-v1"),
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
/// Fair, so a waiting provider gets the model when the current pass ends
/// rather than racing the sync worker's next pass; never poisoned, so one
/// failed pass cannot disable the other provider.
type SharedRuntime = parking_lot::FairMutex<Box<dyn BatchRuntime>>;
type Registry = tokio::sync::Mutex<Vec<(PathBuf, Weak<SharedRuntime>)>>;

/// Models already loaded in this process, by canonical asset directory. A
/// server that composes both the sync worker and MCP retrieval would
/// otherwise hold two identical copies of the F32 weights (about 2.3 GiB
/// each, #739). Entries are weak, so the weights are freed with the last
/// provider. Held across a load so concurrent loads of one directory share.
static LOADED: Registry = tokio::sync::Mutex::const_new(Vec::new());

/// The live model loaded from `key`, or a new one from `load`.
async fn shared<F, Fut>(
    registry: &Registry,
    key: PathBuf,
    load: F,
) -> Result<Arc<SharedRuntime>, EmbeddingError>
where
    F: FnOnce(PathBuf) -> Fut,
    Fut: std::future::Future<Output = Result<Arc<SharedRuntime>, EmbeddingError>>,
{
    let mut loaded = registry.lock().await;
    loaded.retain(|(_, runtime)| runtime.strong_count() > 0);
    if let Some(runtime) = loaded
        .iter()
        .find(|(path, _)| *path == key)
        .and_then(|(_, runtime)| runtime.upgrade())
    {
        return Ok(runtime);
    }
    let runtime = load(key.clone()).await?;
    loaded.push((key, Arc::downgrade(&runtime)));
    Ok(runtime)
}

pub struct QwenProvider {
    runtime: Arc<SharedRuntime>,
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

#[derive(serde::Deserialize)]
struct TensorEntry {
    dtype: String,
    shape: Vec<usize>,
    data_offsets: [usize; 2],
}

/// Streams a safetensors file once, hashing every byte as it is read and
/// building each F32 tensor from exactly those bytes. Only one source tensor
/// is buffered at a time, so the file is never held in memory beside the
/// converted weights. Any digest mismatch, truncation, trailing data or
/// malformed layout drops every built tensor and fails closed.
fn checked_safetensors(
    path: &Path,
    expected: &str,
) -> Result<std::collections::HashMap<String, candle_core::Tensor>, EmbeddingError> {
    use std::io::Read;
    let bad = |_| EmbeddingError::UnsupportedModel;
    let file = std::fs::File::open(path).map_err(|_| EmbeddingError::Unavailable)?;
    // Bound every allocation by the bytes the open file can supply, so a
    // tampered header fails closed instead of exhausting memory.
    let file_len = file
        .metadata()
        .map_err(|_| EmbeddingError::Unavailable)?
        .len();
    let mut reader = std::io::BufReader::with_capacity(1 << 20, file);
    let mut hasher = Sha256::new();
    let mut read = |len: usize| -> Result<Vec<u8>, EmbeddingError> {
        let mut buf = vec![0; len];
        reader
            .read_exact(&mut buf)
            .map_err(|_| EmbeddingError::UnsupportedModel)?;
        hasher.update(&buf);
        Ok(buf)
    };
    let header_len = u64::from_le_bytes(
        read(8)?
            .try_into()
            .map_err(|_| EmbeddingError::UnsupportedModel)?,
    );
    if header_len > 100 << 20 || header_len > file_len.saturating_sub(8) {
        return Err(EmbeddingError::UnsupportedModel);
    }
    let header: std::collections::HashMap<String, serde_json::Value> =
        serde_json::from_slice(&read(header_len as usize)?).map_err(bad)?;
    let mut entries = header
        .into_iter()
        .filter(|(name, _)| name != "__metadata__")
        .map(|(name, v)| Ok((name, serde_json::from_value::<TensorEntry>(v).map_err(bad)?)))
        .collect::<Result<Vec<_>, EmbeddingError>>()?;
    entries.sort_by_key(|(_, e)| e.data_offsets);
    let data_len = (file_len - 8 - header_len) as usize;
    let mut position = 0;
    let mut tensors = std::collections::HashMap::with_capacity(entries.len());
    for (name, entry) in entries {
        let [start, end] = entry.data_offsets;
        let dtype = match entry.dtype.as_str() {
            "BF16" => DType::BF16,
            "F16" => DType::F16,
            "F32" => DType::F32,
            _ => return Err(EmbeddingError::UnsupportedModel),
        };
        let elements = entry
            .shape
            .iter()
            .try_fold(1usize, |n, &d| n.checked_mul(d));
        if start != position
            || end < start
            || end > data_len
            || elements.and_then(|n| n.checked_mul(dtype.size_in_bytes())) != Some(end - start)
        {
            return Err(EmbeddingError::UnsupportedModel);
        }
        let bytes = read(end - start)?;
        let tensor =
            candle_core::Tensor::from_raw_buffer(&bytes, dtype, &entry.shape, &Device::Cpu)
                .and_then(|t| t.to_dtype(DType::F32))
                .map_err(|_| EmbeddingError::UnsupportedModel)?;
        tensors.insert(name, tensor);
        position = end;
    }
    let mut trailing = [0u8; 1];
    if reader
        .read(&mut trailing)
        .map_err(|_| EmbeddingError::UnsupportedModel)?
        != 0
        || format!("{:x}", hasher.finalize()) != expected
    {
        return Err(EmbeddingError::UnsupportedModel);
    }
    Ok(tensors)
}

/// Return heap freed during model load to the OS so peak load memory does not
/// linger in the resident set. Works alongside the arena cap (#659).
fn release_freed_heap() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: malloc_trim only releases free allocator memory; it takes no pointers.
    unsafe {
        libc::malloc_trim(0);
    }
}
impl QwenProvider {
    /// Reads only caller-provided files; never downloads, reads credentials or
    /// initializes an ONNX runtime. Requires a Tokio runtime for blocking work.
    ///
    /// Providers loaded from the same asset directory share one verified
    /// model and tokenizer. Each provider keeps its own admission, so a
    /// query is never refused because the sync worker is embedding; the two
    /// take turns on the shared model one forward pass at a time.
    pub async fn load(config: QwenConfig) -> Result<Self, EmbeddingError> {
        config.validate()?;
        let key = std::fs::canonicalize(&config.assets).map_err(|_| EmbeddingError::Unavailable)?;
        let runtime = shared(&LOADED, key, Self::load_runtime).await?;
        Ok(Self {
            runtime,
            admission: Arc::new(Semaphore::new(1)),
            config,
            metadata: metadata(),
        })
    }
    async fn load_runtime(assets: PathBuf) -> Result<Arc<SharedRuntime>, EmbeddingError> {
        let runtime = tokio::task::spawn_blocking(move || {
            // Verify precisely the bytes subsequently parsed/loaded; weights are
            // hashed while streaming into tensors and discarded on mismatch.
            let cfg_bytes = checked_asset(&assets.join(ASSETS[0].0), ASSETS[0].1)?;
            let tokenizer_bytes = checked_asset(&assets.join(ASSETS[1].0), ASSETS[1].1)?;
            let weights = checked_safetensors(&assets.join(ASSETS[2].0), ASSETS[2].1)?;
            let cfg: Qwen3Config =
                serde_json::from_slice(&cfg_bytes).map_err(|_| EmbeddingError::UnsupportedModel)?;
            if cfg.hidden_size != DIMENSION {
                return Err(EmbeddingError::UnsupportedModel);
            }
            let vb = VarBuilder::from_tensors(weights, DType::F32, &Device::Cpu);
            let model = Qwen3Model::new(cfg, vb).map_err(|_| EmbeddingError::Unavailable)?;
            release_freed_heap();
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
        Ok(Arc::new(SharedRuntime::new(Box::new(runtime))))
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
const MAX_TEXT_BYTES: usize = 32768;

fn validate_inputs(inputs: &[String]) -> Result<(), EmbeddingError> {
    // Bound cloning/tokenization before admission. Caller can split large jobs.
    if inputs.len() > 32
        || inputs.iter().any(|input| input.len() > MAX_TEXT_BYTES)
        || inputs.iter().map(String::len).sum::<usize>() > 262144
    {
        return Err(EmbeddingError::InputLimit);
    }
    if inputs.iter().any(|input| input.trim().is_empty()) {
        return Err(EmbeddingError::InvalidInput);
    }
    Ok(())
}
impl EmbeddingProvider for QwenProvider {
    fn wait_idle(&self) -> notion_knowledge_core::embedding::EmbeddingIdleFuture<'_> {
        Box::pin(async move {
            let permit = self
                .admission
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| EmbeddingError::Unavailable)?;
            drop(permit);
            Ok(())
        })
    }
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn partition_input<'a>(
        &'a self,
        input: &'a str,
        boundaries: Vec<usize>,
    ) -> EmbeddingPartitionFuture<'a> {
        Box::pin(async move {
            let permit = self
                .admission
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| EmbeddingError::Unavailable)?;
            let input = input.to_owned();
            let runtime = self.runtime.clone();
            let max_tokens = self.config.max_tokens;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                partition_at_boundaries(&input, &boundaries, |part| {
                    if part.len() > MAX_TEXT_BYTES {
                        return Err(EmbeddingError::InputLimit);
                    }
                    if part.trim().is_empty() {
                        return Err(EmbeddingError::InvalidInput);
                    }
                    let tokens = runtime.lock().token_count(part)?;
                    if tokens == 0 {
                        return Err(EmbeddingError::InvalidInput);
                    }
                    if tokens > max_tokens {
                        return Err(EmbeddingError::InputLimit);
                    }
                    Ok(())
                })
            })
            .await
            .map_err(|_| EmbeddingError::Unavailable)?
        })
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
                let lock = || runtime.lock();
                let counter = lock();
                // Validate all inputs before any forward pass, without truncation.
                let lengths: Vec<_> = inputs
                    .iter()
                    .map(|s| counter.token_count(s))
                    .collect::<Result<_, _>>()?;
                drop(counter);
                if lengths.contains(&0) {
                    return Err(EmbeddingError::InvalidInput);
                }
                if lengths.iter().any(|&n| n > config.max_tokens) {
                    return Err(EmbeddingError::InputLimit);
                }
                if lengths
                    .chunks(config.batch_size)
                    .any(|tokens| tokens.iter().max().copied().unwrap_or(0) * tokens.len() > 4096)
                {
                    return Err(EmbeddingError::InputLimit);
                }
                let mut output = Vec::with_capacity(inputs.len());
                for batch in inputs.chunks(config.batch_size) {
                    // Lock per forward pass so another provider sharing the
                    // model waits for at most one pass, not a whole request.
                    let vectors = lock().embed(batch)?;
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
    use std::sync::Mutex;
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
        for text in ["".to_string(), " \n".to_string()] {
            assert_eq!(validate_inputs(&[text]), Err(EmbeddingError::InvalidInput));
        }
        assert_eq!(
            validate_inputs(&["å".repeat(32769)]),
            Err(EmbeddingError::InputLimit)
        );
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
            Err(EmbeddingError::InputLimit)
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
    fn safetensors_fixture(name: &str) -> (PathBuf, Vec<u8>) {
        let a = candle_core::Tensor::new(&[[1.5f32, -2.25], [3.0, 0.1]], &Device::Cpu)
            .unwrap()
            .to_dtype(DType::BF16)
            .unwrap();
        let b = candle_core::Tensor::new(&[7.0f32, f32::MIN_POSITIVE, -0.0], &Device::Cpu).unwrap();
        let p = std::env::temp_dir().join(format!("nk665-{name}-{}", std::process::id()));
        candle_core::safetensors::save(
            &std::collections::HashMap::from([("a".to_string(), a), ("b".to_string(), b)]),
            &p,
        )
        .unwrap();
        let bytes = std::fs::read(&p).unwrap();
        (p, bytes)
    }
    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
    #[test]
    fn streamed_weights_match_buffered_f32_load_bit_for_bit() {
        let (p, bytes) = safetensors_fixture("same");
        let streamed = checked_safetensors(&p, &digest(&bytes)).unwrap();
        let buffered =
            VarBuilder::from_buffered_safetensors(bytes, DType::F32, &Device::Cpu).unwrap();
        assert_eq!(streamed.len(), 2);
        for (name, shape) in [("a", vec![2, 2]), ("b", vec![3])] {
            let expected = buffered.get(shape.as_slice(), name).unwrap();
            let actual = &streamed[name];
            assert_eq!(actual.dtype(), DType::F32);
            let bits = |t: &candle_core::Tensor| {
                t.flatten_all()
                    .unwrap()
                    .to_vec1::<f32>()
                    .unwrap()
                    .iter()
                    .map(|x| x.to_bits())
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(actual), bits(&expected));
        }
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn tampered_truncated_or_extended_weights_fail_closed() {
        let (p, bytes) = safetensors_fixture("tamper");
        let expected = digest(&bytes);
        let mut tampered = bytes.clone();
        *tampered.last_mut().unwrap() ^= 1;
        let mut extended = bytes.clone();
        extended.push(0);
        for variant in [
            tampered,
            bytes[..bytes.len() - 1].to_vec(),
            bytes[..4].to_vec(),
            extended,
        ] {
            std::fs::write(&p, &variant).unwrap();
            assert_eq!(
                checked_safetensors(&p, &expected).err(),
                Some(EmbeddingError::UnsupportedModel)
            );
        }
        // A header claiming more data than the file holds is rejected before
        // allocating, even though the digest would also fail later.
        let header =
            br#"{"a":{"dtype":"F32","shape":[1099511627776],"data_offsets":[0,4398046511104]}}"#;
        let mut oversized = (header.len() as u64).to_le_bytes().to_vec();
        oversized.extend_from_slice(header);
        oversized.extend_from_slice(&[0; 16]);
        std::fs::write(&p, &oversized).unwrap();
        assert_eq!(
            checked_safetensors(&p, &digest(&oversized)).err(),
            Some(EmbeddingError::UnsupportedModel)
        );
        std::fs::write(&p, &bytes).unwrap();
        assert_eq!(
            checked_safetensors(&p, ASSETS[2].1).err(),
            Some(EmbeddingError::UnsupportedModel)
        );
        std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn previous_candle_runtime_identity_requires_index_rebuild() {
        let current = metadata();
        let previous = EmbeddingMetadata::new(
            current.provider_id().into(),
            current.model_id().into(),
            format!("{REVISION}:fastembed7.1.0:candle0.11.0:tokenizers0.23.2:last-token:left-padding:l2:exact-text:untruncated-v1"),
            DIMENSION,
        )
        .unwrap();
        assert_eq!(
            current.ensure_compatible(&previous),
            Err(EmbeddingError::IncompatibleIndex)
        );
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
                runtime: Arc::new(SharedRuntime::new(Box::new(Fake {
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
    async fn cpu_context_admits_attachment_sized_text_without_truncation() {
        let (provider, calls) = fake(QwenConfig::cpu("unused"), false);
        // The runtime reports token counts independently of character length.
        // UUID-heavy attachment Markdown can exceed 512 tokens below 1,000 chars.
        let texts = ["530", "768", "1024"].map(str::to_owned);
        let vectors = provider.embed_batch(&texts).await.unwrap();
        assert_eq!(vectors.len(), texts.len());
        assert_eq!(
            *calls.lock().unwrap(),
            texts.iter().cloned().map(|s| vec![s]).collect::<Vec<_>>()
        );
        assert_eq!(
            provider.embed_batch(&["1025".into()]).await,
            Err(EmbeddingError::InputLimit)
        );
        assert_eq!(calls.lock().unwrap().len(), texts.len());

        let mut smaller = QwenConfig::cpu("unused");
        smaller.max_tokens = 512;
        let (provider, calls) = fake(smaller, false);
        assert_eq!(
            provider.embed_batch(&["530".into()]).await,
            Err(EmbeddingError::InputLimit)
        );
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn indexing_partitions_use_the_same_admission_as_inference() {
        let (provider, calls) = fake(QwenConfig::cpu("unused"), false);
        let query_admission = provider.admission.clone().acquire_owned().await.unwrap();
        let partition = provider.partition_input("600500", vec![3]);
        tokio::pin!(partition);
        tokio::select! {
            biased;
            result = &mut partition => panic!("partition should wait for admission: {result:?}"),
            _ = tokio::task::yield_now() => {}
        }
        drop(query_admission);
        assert_eq!(partition.await.unwrap(), [3]);
        assert!(
            provider
                .partition_input("1024", vec![2])
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            provider.partition_input("32000", vec![4]).await,
            Err(EmbeddingError::InputLimit)
        );
        assert!(calls.lock().unwrap().is_empty());
        assert!(provider.embed_batch(&["600".into()]).await.is_ok());
        assert!(provider.embed_batch(&["500".into()]).await.is_ok());
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
            Err(EmbeddingError::InputLimit)
        );
        assert_eq!(
            p.embed_batch(&["1".into(), "3000".into()]).await,
            Err(EmbeddingError::InputLimit)
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
            if let Some(started) = self.started.lock().unwrap().take() {
                started.send(()).unwrap();
            }
            self.release.lock().unwrap().recv().unwrap();
            Ok(vec![vec![0.0; DIMENSION]; inputs.len()])
        }
    }
    #[tokio::test]
    async fn one_live_model_is_shared_per_asset_directory() {
        let registry = Registry::const_new(Vec::new());
        let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let load = |_: PathBuf| {
            let loads = loads.clone();
            async move {
                loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let runtime: Box<dyn BatchRuntime> = Box::new(Fake {
                    calls: Arc::default(),
                    malformed: false,
                });
                Ok(Arc::new(SharedRuntime::new(runtime)))
            }
        };
        let count = || loads.load(std::sync::atomic::Ordering::SeqCst);
        let a = shared(&registry, "/assets/a".into(), load).await.unwrap();
        let again = shared(&registry, "/assets/a".into(), load).await.unwrap();
        assert!(Arc::ptr_eq(&a, &again));
        assert_eq!(count(), 1);
        let b = shared(&registry, "/assets/b".into(), load).await.unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(count(), 2);
        // The registry never keeps a model alive on its own.
        drop((a, again));
        let reloaded = shared(&registry, "/assets/a".into(), load).await.unwrap();
        assert_eq!(count(), 3);
        assert_eq!(registry.lock().await.len(), 2);
        drop((reloaded, b));
        let failing = |_: PathBuf| async { Err(EmbeddingError::UnsupportedModel) };
        assert_eq!(
            shared(&registry, "/assets/a".into(), failing).await.err(),
            Some(EmbeddingError::UnsupportedModel)
        );
        assert!(registry.lock().await.is_empty());
    }
    #[tokio::test]
    async fn providers_sharing_a_model_admit_independently_and_take_turns() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let runtime: Arc<SharedRuntime> = Arc::new(SharedRuntime::new(Box::new(Blocking {
            started: Mutex::new(Some(started_tx)),
            release: Mutex::new(release_rx),
        })));
        let handle = || {
            Arc::new(QwenProvider {
                runtime: runtime.clone(),
                admission: Arc::new(Semaphore::new(1)),
                config: QwenConfig::cpu("unused"),
                metadata: metadata(),
            })
        };
        let (sync, query) = (handle(), handle());
        let running = sync.clone();
        let first = tokio::spawn(async move { running.embed_batch(&["page".into()]).await });
        started_rx.await.unwrap();
        // The sync handle is busy, but the query handle is admitted and waits
        // for the shared model instead of failing with ResourceExhausted.
        let waiting = query.clone();
        let second = tokio::spawn(async move { waiting.embed_batch(&["query".into()]).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!second.is_finished());
        assert_eq!(query.admission.available_permits(), 0);
        release_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap().len(), 1);
        // Blocking only signals its first pass; later passes still need a release.
        release_tx.send(()).unwrap();
        assert_eq!(second.await.unwrap().unwrap().len(), 1);
    }
    #[tokio::test]
    async fn canceled_caller_retains_admission_until_blocking_work_finishes() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let p = Arc::new(QwenProvider {
            runtime: Arc::new(SharedRuntime::new(Box::new(Blocking {
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
        let idle = p.wait_idle();
        tokio::pin!(idle);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut idle)
                .await
                .is_err()
        );
        release_tx.send(()).unwrap();
        idle.await.unwrap();
        let permit = p.admission.clone().acquire_owned().await.unwrap();
        drop(permit);
        assert_eq!(p.admission.available_permits(), 1);
    }
}
