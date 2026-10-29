//! Trusted, credential-free benchmark of the production committed retrieval paths.
mod production_support;
use notion_knowledge_retrieval::{
    chunks::lexical_binding::LexicalCachePlan,
    lexical_dictionary::Dictionary,
    qwen::{QwenConfig, QwenProvider},
};
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
    // Trusted operator environment only; the evaluation request cannot choose assets.
    // Verify these before loading the larger embedding model or creating cache state.
    let lexical = match std::env::var("NK_LEXICAL_EXPANSION").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("false") => None,
        Ok("true") => {
            let path = std::env::var_os("NK_LEXICAL_ASSET_BUNDLE")
                .map(std::path::PathBuf::from)
                .filter(|p| p.is_absolute())
                .ok_or("lexical bundle requires an absolute local path")?;
            let digest = std::env::var("NK_LEXICAL_ASSET_SHA256")
                .map_err(|_| "lexical bundle requires a configured digest")?;
            let dictionary = Dictionary::load(&path, &digest)
                .map_err(|_| "lexical assets unavailable or incompatible")?;
            Some(LexicalCachePlan::verified(Arc::new(dictionary))?)
        }
        _ => return Err("NK_LEXICAL_EXPANSION must be true or false".into()),
    };
    let provider = Arc::new(QwenProvider::load(QwenConfig::cpu(assets)).await?);
    let output = if lexical.is_some() {
        production_support::run_with_lexical(
            provider,
            std::path::Path::new(&state),
            request,
            lexical,
        )
        .await?
    } else {
        production_support::run(provider, std::path::Path::new(&state), request).await?
    };
    serde_json::to_writer(std::io::stdout(), &output)?;
    Ok(())
}
