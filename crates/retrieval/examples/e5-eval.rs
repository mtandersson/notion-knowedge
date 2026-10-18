//! Offline comparison only: this provider is not installed by the server.
mod eval_support;
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config};
use notion_knowledge_core::embedding::{
    EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider,
};
use sha2::{Digest, Sha256};
use std::path::Path;

const REVISION: &str = "614241f622f53c4eeff9890bdc4f31cfecc418b3";
const ASSETS: [(&str, &str); 3] = [
    (
        "config.json",
        "69137736cab8b8903a07fe8afaafdda25aac55415a12a55d1bffa9f581abf959",
    ),
    (
        "tokenizer.json",
        "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39",
    ),
    (
        "model.safetensors",
        "1a55775f53449dac10a2bcbc312469fac40b96d53198c407081a831f81c98477",
    ),
];

struct E5 {
    model: BertModel,
    tokenizer: tokenizers::Tokenizer,
    metadata: EmbeddingMetadata,
}
fn checked_asset(path: &Path, digest: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    if format!("{:x}", Sha256::digest(&bytes)) != digest {
        return Err("unsupported E5 asset digest".into());
    }
    Ok(bytes)
}
fn check_token_count(count: usize) -> Result<(), EmbeddingError> {
    if !(1..=512).contains(&count) {
        return Err(EmbeddingError::InvalidInput);
    }
    Ok(())
}
// Follow the upstream masked mean pooling, then normalize to unit length.
fn pool(hidden: &Tensor, mask: &Tensor) -> candle_core::Result<Vec<f32>> {
    let mask = mask.to_dtype(DType::F32)?.unsqueeze(2)?;
    let sum = hidden.broadcast_mul(&mask)?.sum(1)?;
    let count = mask.sum(1)?;
    let mean = sum.broadcast_div(&count)?;
    let norm = mean.sqr()?.sum_keepdim(1)?.sqrt()?;
    mean.broadcast_div(&norm)?.squeeze(0)?.to_vec1()
}
impl E5 {
    fn load(assets: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let config = checked_asset(&assets.join(ASSETS[0].0), ASSETS[0].1)?;
        let vocabulary = checked_asset(&assets.join(ASSETS[1].0), ASSETS[1].1)?;
        let weights = checked_asset(&assets.join(ASSETS[2].0), ASSETS[2].1)?;
        let config: Config = serde_json::from_slice(&config)?;
        if config.hidden_size != 384 || config.max_position_embeddings != 512 {
            return Err("unsupported E5 model configuration".into());
        }
        let vb = VarBuilder::from_buffered_safetensors(weights, DType::F32, &Device::Cpu)?;
        let model = BertModel::load(vb, &config)?;
        let mut tokenizer =
            tokenizers::Tokenizer::from_bytes(vocabulary).map_err(|_| "invalid E5 vocabulary")?;
        tokenizer
            .with_truncation(None)
            .map_err(|_| "invalid E5 truncation policy")?;
        tokenizer.with_padding(None);
        Ok(Self {
            model,
            tokenizer,
            metadata: EmbeddingMetadata::new(
                "eval-candle-cpu-f32".into(),
                "intfloat/multilingual-e5-small".into(),
                format!(
                    "{REVISION}:candle0.11.0:tokenizers0.23.2:masked-mean:l2:query-passage-prefix:untruncated-v1"
                ),
                384,
            )?,
        })
    }
    fn encode(&self, text: &str) -> Result<Vec<f32>, EmbeddingError> {
        if text.len() > 32 * 1024 {
            return Err(EmbeddingError::InvalidInput);
        }
        let encoding = self
            .tokenizer
            .encode(text, true)
            .map_err(|_| EmbeddingError::InvalidInput)?;
        check_token_count(encoding.len())?;
        let calculate = || -> candle_core::Result<Vec<f32>> {
            let ids = Tensor::new(encoding.get_ids(), &Device::Cpu)?.unsqueeze(0)?;
            let types = Tensor::new(encoding.get_type_ids(), &Device::Cpu)?.unsqueeze(0)?;
            let mask = Tensor::new(encoding.get_attention_mask(), &Device::Cpu)?.unsqueeze(0)?;
            let hidden = self.model.forward(&ids, &types, Some(&mask))?;
            pool(&hidden, &mask)
        };
        calculate().map_err(|_| EmbeddingError::Unavailable)
    }
}
impl EmbeddingProvider for E5 {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.metadata
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            if inputs.len() > 1 {
                return Err(EmbeddingError::ResourceExhausted);
            }
            inputs.iter().map(|text| self.encode(text)).collect()
        })
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (assets, measurements, request) = eval_support::arguments()?;
    let started = std::time::Instant::now();
    let provider = E5::load(Path::new(&assets))?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;
    eval_support::run(
        &provider,
        &measurements,
        request,
        load_ms,
        "passage: ",
        "query: ",
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mean_pooling_excludes_padding_before_unit_normalization() {
        let hidden = Tensor::new(&[[[1f32, 0.], [0., 1.], [100., 0.]]], &Device::Cpu).unwrap();
        let mask = Tensor::new(&[[1u32, 1, 0]], &Device::Cpu).unwrap();
        let pooled = pool(&hidden, &mask).unwrap();
        for value in pooled {
            assert!((value - std::f32::consts::FRAC_1_SQRT_2).abs() < 1e-6);
        }
    }
    #[test]
    fn overlength_inputs_are_rejected_instead_of_truncated() {
        assert!(check_token_count(1).is_ok());
        assert!(check_token_count(512).is_ok());
        assert!(check_token_count(0).is_err());
        assert!(check_token_count(513).is_err());
    }
}
