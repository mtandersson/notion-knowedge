//! Fail-closed command-line scope parsing, before credential use or network I/O.
use notion_knowledge_core::{
    backend::PageId,
    discovery::{ExclusionRules, SourceType},
};
use notion_knowledge_notion::pages::page_id;

pub fn parse(args: &[String]) -> Result<(Vec<PageId>, ExclusionRules), &'static str> {
    let mut roots = Vec::new();
    let mut rules = ExclusionRules::default();
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--exclude-page" | "--exclude-descendants" => {
                let id = page_id(args.next().ok_or("Missing exclusion ID")?)
                    .map_err(|_| "Invalid exclusion ID")?
                    .0;
                if arg == "--exclude-page" {
                    rules.page_ids.insert(id);
                } else {
                    rules.descendants_of.insert(id);
                }
            }
            "--exclude-source-type" => {
                rules
                    .source_types
                    .insert(match args.next().map(String::as_str) {
                        Some("page") => SourceType::Page,
                        Some("database") => SourceType::Database,
                        Some("data_source") => SourceType::DataSource,
                        _ => return Err("Unknown or missing source type"),
                    });
            }
            _ if arg.starts_with('-') => return Err("Unknown discovery option"),
            _ => roots.push(page_id(arg).map_err(|_| "Invalid root ID")?),
        }
    }
    if roots.is_empty() {
        return Err("At least one explicit root is required");
    }
    Ok((roots, rules))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scope_options_are_normalized_and_invalid_or_missing_authority_is_rejected() {
        let id = "00000000000000000000000000000001";
        let args = [
            id,
            "--exclude-page",
            id,
            "--exclude-descendants",
            id,
            "--exclude-source-type",
            "database",
        ]
        .map(String::from);
        let (roots, rules) = parse(&args).unwrap();
        assert!(rules.page_ids.contains(&roots[0].0));
        assert!(rules.descendants_of.contains(&roots[0].0));
        assert!(rules.source_types.contains(&SourceType::Database));
        for args in [
            vec![],
            vec!["--exclude-page", id],
            vec![id, "--exclude-page"],
            vec![id, "--exclude-source-type", "unknown"],
            vec![id, "--typo"],
            vec!["invalid"],
        ] {
            assert!(parse(&args.into_iter().map(String::from).collect::<Vec<_>>()).is_err());
        }
    }
}
