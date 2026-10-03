//! Explicit read-only smoke: credentials stay in the process environment.
use notion_knowledge_core::{chunking::ChunkConfig, discovery::ExclusionRules};
use notion_knowledge_notion::NotionClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let root = args
        .next()
        .ok_or("expected ROOT WORKSPACE [EXCLUDED_PAGE ...]")?;
    let workspace = args
        .next()
        .ok_or("expected ROOT WORKSPACE [EXCLUDED_PAGE ...]")?;
    let mut rules = ExclusionRules::default();
    for excluded in args {
        rules
            .page_ids
            .insert(notion_knowledge_notion::pages::page_id(&excluded)?.0);
    }
    let token = std::env::var("NOTION_TOKEN").map_err(|_| "NOTION_TOKEN is required")?;
    let client = NotionClient::integration(&token)?;
    let snapshot = client
        .discover_documents(&root, &workspace, &rules, ChunkConfig::default())
        .await?;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}
