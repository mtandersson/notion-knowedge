//! Opt-in real local model smoke; no private source or external credentials.
use notion_knowledge_core::embedding::{self, EmbeddingProvider};
use notion_knowledge_retrieval::qwen::{QwenConfig, QwenProvider};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let assets = args
        .next()
        .ok_or("usage: qwen-smoke ASSETS NEW_METADATA_PATH [BATCH_SIZE]")?;
    let metadata = args.next().ok_or("missing metadata path")?;
    let mut config = QwenConfig::cpu(assets);
    config.batch_size = args.next().map(|s| s.parse()).transpose()?.unwrap_or(1);
    let start = std::time::Instant::now();
    let provider = QwenProvider::load(config).await?;
    println!("model_load_ms={}", start.elapsed().as_millis());
    provider.persist_metadata(std::path::Path::new(&metadata))?;
    let persisted: embedding::EmbeddingMetadata =
        serde_json::from_slice(&std::fs::read(metadata)?)?;
    provider.metadata().ensure_compatible(&persisted)?;
    let inputs = [
        "Katten sover på soffan.",
        "The cat sleeps on the sofa.",
        "A database backup protects stored information.",
        "Katten sover på soffan.",
    ]
    .map(str::to_string);
    let start = std::time::Instant::now();
    let vectors = embedding::embed_batch(&provider, &inputs).await?;
    let dot = |a: usize, b: usize| {
        vectors[a]
            .iter()
            .zip(&vectors[b])
            .map(|(x, y)| x * y)
            .sum::<f32>()
    };
    let related = dot(0, 1);
    let unrelated = dot(0, 2);
    if related <= unrelated || vectors[0] != vectors[3] {
        return Err("sample comparison or duplicate order failed".into());
    }
    println!(
        "embed_ms={} vectors={} dimension={} related={related:.6} unrelated={unrelated:.6} duplicate_equal=true identity_roundtrip=true",
        start.elapsed().as_millis(),
        vectors.len(),
        provider.metadata().dimension()
    );
    Ok(())
}
