//! Opt-in real pinned Qwen baseline; shares corpus/ranking protocol with E5.
mod eval_support;
use notion_knowledge_retrieval::qwen::{QwenConfig, QwenProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (assets, measurements, request) = eval_support::arguments()?;
    let started = std::time::Instant::now();
    let provider = QwenProvider::load(QwenConfig::cpu(assets)).await?;
    let load_ms = started.elapsed().as_secs_f64() * 1000.0;
    eval_support::run(&provider, &measurements, request, load_ms, "", "").await
}
