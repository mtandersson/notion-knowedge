use std::env;

fn components() -> [&'static str; 4] {
    [
        notion_knowledge_core::COMPONENT,
        notion_knowledge_notion::COMPONENT,
        notion_knowledge_retrieval::COMPONENT,
        notion_knowledge_mcp::COMPONENT,
    ]
}

fn main() {
    let check_only = env::args().skip(1).any(|arg| arg == "--check");

    eprintln!(
        "notion-knowledge bootstrap ready ({})",
        components().join(", ")
    );

    if check_only {
        return;
    }

    eprintln!(
        "No MCP transport is wired yet; stdio and Streamable HTTP are implemented by #17 and #18."
    );
    eprintln!("Bootstrap process is running; press Ctrl-C to stop.");

    loop {
        std::thread::park();
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
