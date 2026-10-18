//! Trusted, credential-free benchmark of the production committed retrieval paths.
mod production_support;
use notion_knowledge_retrieval::qwen::{QwenConfig, QwenProvider};
use std::{io::Read, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let assets = args
        .next()
        .ok_or("usage: production-eval ASSETS STATE_DIRECTORY")?;
    let state = args.next().ok_or("missing state directory")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let mut raw = String::new();
    std::io::stdin()
        .take(8 * 1024 * 1024 + 1)
        .read_to_string(&mut raw)?;
    if raw.len() > 8 * 1024 * 1024 {
        return Err("request too large".into());
    }
    let request = production_support::parse(&raw)?;
    let provider = Arc::new(QwenProvider::load(QwenConfig::cpu(assets)).await?);
    let output = production_support::run(provider, std::path::Path::new(&state), request).await?;
    serde_json::to_writer(std::io::stdout(), &output)?;
    Ok(())
}
