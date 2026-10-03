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
    let args: Vec<_> = env::args().skip(1).collect();
    if let Some(position) = args.iter().position(|arg| arg == "--crawl-dry-run") {
        let roots = match args[position + 1..]
            .iter()
            .map(|root| notion_knowledge_notion::pages::page_id(root))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(roots) if !roots.is_empty() => roots,
            _ => {
                eprintln!("--crawl-dry-run requires one or more Notion root page IDs or links.");
                return ExitCode::from(2);
            }
        };
        let notion_knowledge_server::config::NotionAuth::Integration(token) = &config.notion_auth
        else {
            eprintln!("Discovery requires NK_NOTION_AUTH=integration.");
            return ExitCode::from(2);
        };
        let client = match notion_knowledge_notion::NotionClient::integration(token.expose_secret())
        {
            Ok(client) => client,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        return match client.crawl_roots(&roots).await {
            Ok(report) => {
                // Output intentionally includes discovered titles/links; no
                // content is printed before the complete read-only run succeeds.
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).expect("serializable discovery")
                );
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }
    if env::args().skip(1).any(|arg| arg == "--notion-identity") {
        let notion_knowledge_server::config::NotionAuth::Integration(token) = &config.notion_auth
        else {
            eprintln!("Notion identity requires NK_NOTION_AUTH=integration.");
            return ExitCode::from(2);
        };
        let client = match notion_knowledge_notion::NotionClient::integration(token.expose_secret())
        {
            Ok(client) => client,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        return match client.identity().await {
            Ok(_) => {
                println!("Notion integration identity verified.");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::FAILURE
            }
        };
    }
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
