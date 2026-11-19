//! Opt-in local throughput benchmark for Qwen inference batch sizes.
//! Reads newline-delimited JSON strings (one chunk text per line), embeds them
//! with the given batch size, prints chunks/s and writes raw f32 vectors so runs
//! can be compared for vector identity. No Notion or credentials involved.
use notion_knowledge_core::embedding::{self};
use notion_knowledge_retrieval::qwen::{QwenConfig, QwenProvider};
use std::io::Write;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: qwen-batch-bench ASSETS INPUTS_JSONL BATCH_SIZE VECTORS_OUT";
    let assets = args.next().ok_or(usage)?;
    let inputs = args.next().ok_or(usage)?;
    let batch: usize = args.next().ok_or(usage)?.parse()?;
    let out = args.next().ok_or(usage)?;
    let texts: Vec<String> = std::fs::read_to_string(inputs)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let mut config = QwenConfig::cpu(assets);
    config.batch_size = batch;
    let provider = QwenProvider::load(config).await?;
    let start = std::time::Instant::now();
    let mut vectors = Vec::with_capacity(texts.len());
    for call in texts.chunks(32) {
        vectors.extend(embedding::embed_batch(&provider, call).await?);
    }
    let secs = start.elapsed().as_secs_f64();
    let mut file = std::io::BufWriter::new(std::fs::File::create(out)?);
    for v in &vectors {
        for x in v {
            file.write_all(&x.to_le_bytes())?;
        }
    }
    println!(
        "batch_size={batch} chunks={} embed_s={secs:.2} chunks_per_s={:.3}",
        vectors.len(),
        vectors.len() as f64 / secs
    );
    Ok(())
}
