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
    if env::args().skip(1).any(|arg| arg == "--version") {
        println!(
            "{} {}",
            notion_knowledge_core::SERVER_NAME,
            notion_knowledge_core::VERSION
        );
        return ExitCode::SUCCESS;
    }
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return ExitCode::from(2);
        }
    };
    if env::args().skip(1).any(|arg| arg == "--healthcheck") {
        return match notion_knowledge_server::http::container_healthcheck(&config) {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) | Err(_) => ExitCode::FAILURE,
        };
    }
    if env::args().skip(1).any(|arg| arg == "--diagnostics") {
        println!(
            "{}",
            notion_knowledge_server::diagnostics::report(
                notion_knowledge_server::diagnostics::bootstrap(&config).health(),
                "one-shot"
            )
        );
        return ExitCode::SUCCESS;
    }
    let check_only = env::args().skip(1).any(|arg| arg == "--check");

    eprintln!(
        "notion-knowledge bootstrap ready ({}) version {}",
        components().join(", "),
        notion_knowledge_core::VERSION
    );

    if check_only {
        return ExitCode::SUCCESS;
    }

    if env::args().skip(1).any(|arg| arg == "--http") {
        return match notion_knowledge_server::http::serve(config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("MCP HTTP service failed: {error}");
                ExitCode::FAILURE
            }
        };
    }

    eprintln!("Serving MCP over stdio. [ci-cache-probe-185]");
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
