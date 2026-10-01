use std::env;
use std::process::ExitCode;

use notion_knowledge_server::config::Config;
use rmcp::{ServiceExt, service::QuitReason, transport::stdio};

fn components() -> [&'static str; 4] {
    [
        notion_knowledge_core::COMPONENT,
        notion_knowledge_notion::COMPONENT,
        notion_knowledge_retrieval::COMPONENT,
        notion_knowledge_mcp::COMPONENT,
    ]
}

#[tokio::main]
async fn main() -> ExitCode {
    let _config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return ExitCode::from(2);
        }
    };
    let check_only = env::args().skip(1).any(|arg| arg == "--check");

    eprintln!(
        "notion-knowledge bootstrap ready ({})",
        components().join(", ")
    );

    if check_only {
        return ExitCode::SUCCESS;
    }

    eprintln!("Serving MCP over stdio.");
    let service = match notion_knowledge_mcp::KnowledgeServer.serve(stdio()).await {
        Ok(service) => service,
        Err(_) => {
            eprintln!("MCP stdio initialization failed.");
            return ExitCode::FAILURE;
        }
    };
    match service.waiting().await {
        Ok(QuitReason::Closed | QuitReason::Cancelled) => ExitCode::SUCCESS,
        _ => {
            eprintln!("MCP stdio service failed.");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::components;

    #[test]
    fn composition_root_links_all_architecture_components() {
        assert_eq!(components(), ["core", "notion", "retrieval", "mcp"]);
    }
}
