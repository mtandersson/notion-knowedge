//! Tool inputs must survive both JSON Schema search and connector full matches.
use notion_knowledge_mcp::KnowledgeServer;
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeSet;

fn nonblank_patterns<'a>(value: &'a Value, patterns: &mut Vec<&'a str>) {
    match value {
        Value::Object(fields) => {
            // Exact hash schemas have a fixed 64-character bound; UUID upload
            // targets have their own syntax. This selects bounded nonblank text
            // and opaque IDs, including nested search-filter alternatives.
            if fields.get("minLength").and_then(Value::as_u64) == Some(1)
                && fields
                    .get("maxLength")
                    .and_then(Value::as_u64)
                    .is_some_and(|max| max != 64)
                && let Some(pattern) = fields.get("pattern").and_then(Value::as_str)
            {
                patterns.push(pattern);
            }
            for child in fields.values() {
                nonblank_patterns(child, patterns);
            }
        }
        Value::Array(children) => {
            for child in children {
                nonblank_patterns(child, patterns);
            }
        }
        _ => {}
    }
}

#[test]
fn catalog_accepts_complete_nonblank_text_with_search_or_full_match_validation() {
    let catalog = KnowledgeServer::default()
        .with_read_only(false)
        .with_destructive_writes_enabled(true)
        .with_write_design_preview()
        .tool_catalog();
    let mut covered = BTreeSet::new();
    for tool in catalog {
        let schema = serde_json::to_value(&tool.input_schema).unwrap();
        let mut patterns = Vec::new();
        nonblank_patterns(&schema, &mut patterns);
        if patterns.is_empty() {
            continue;
        }
        covered.insert(tool.name.to_string());
        for pattern in patterns {
            let search = Regex::new(pattern).unwrap();
            let full_match = Regex::new(&format!(r"\A(?:{pattern})\z")).unwrap();
            for text in [
                "x",
                "page-id-123",
                "deployment notes",
                "Hur fungerar återhämtning?",
                " \t source-id \n",
                "# Heading\n\nMultiline Markdown.\r\nSecond paragraph.",
            ] {
                assert!(search.is_match(text), "{} rejected {text:?}", tool.name);
                assert!(
                    full_match.is_match(text),
                    "{} rejected complete {text:?}",
                    tool.name
                );
            }
            for text in ["", " ", "\t\r\n", "\u{2003}\u{00a0}"] {
                assert!(!search.is_match(text), "{} accepted whitespace", tool.name);
                assert!(
                    !full_match.is_match(text),
                    "{} accepted complete whitespace",
                    tool.name
                );
            }
            assert!(pattern.starts_with('^') && pattern.ends_with('$'));
            assert!(!search.is_match(&" ".repeat(200_000)));
        }
    }
    assert_eq!(
        covered,
        [
            "knowledge_search",
            "knowledge_get",
            "knowledge_create_page",
            "knowledge_append",
            "knowledge_update_section",
            "knowledge_archive_page",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
}
