use notion_knowledge_core::{
    chunking::{ChunkConfig, chunk_document},
    indexed::{IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
};
use std::collections::BTreeMap;
fn document(text: &str) -> IndexedDocument {
    IndexedDocument {
        schema_version: SchemaVersion::V1,
        metadata: IndexedMetadata {
            page_id: "page".into(),
            block_id: None,
            url: "https://example.org".into(),
            title: "Document".into(),
            heading_path: vec![],
            last_edited_time: "timestamp".into(),
            source: SourceMetadata {
                workspace_id: "workspace".into(),
                root_page_id: "root".into(),
                database_id: None,
                data_source_id: None,
            },
            properties: BTreeMap::new(),
        },
        text: text.into(),
        content_hash: "document-hash".into(),
        links: vec![],
    }
}
#[test]
fn short_sections_preserve_heading_hierarchy_and_exact_source() {
    let doc = document("intro\n\n# **One**\n\nshort\n\n### Deep\n\n日本語\n\n## Sibling\n\nlast\n");
    let chunks = chunk_document(&doc, ChunkConfig::default()).unwrap();
    assert_eq!(
        chunks
            .iter()
            .map(|c| c.metadata.heading_path.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![],
            vec!["One"],
            vec!["One", "Deep"],
            vec!["One", "Sibling"]
        ]
    );
    assert_eq!(
        chunks.iter().map(|c| c.text.as_str()).collect::<String>(),
        doc.text
    );
    for c in chunks {
        assert_eq!(c.text, doc.text[c.source_bytes]);
        assert_eq!(c.metadata.page_id, "page");
    }
}
#[test]
fn long_plain_prose_is_bounded_deterministic_and_overlap_is_small() {
    let doc = document(&format!("# Heading\n\n{}", "åäö word prose ".repeat(100)));
    let config = ChunkConfig {
        target_chars: 100,
        overlap_chars: 50,
    };
    let chunks = chunk_document(&doc, config).unwrap();
    assert!(chunks.len() > 10);
    assert_eq!(chunks, chunk_document(&doc, config).unwrap());
    for c in &chunks {
        assert!(c.text.chars().count() <= 100);
        assert_eq!(c.metadata.heading_path, vec!["Heading"]);
        assert_eq!(c.text, doc.text[c.source_bytes.clone()]);
    }
    assert_eq!(chunks[0].source_bytes.start, 0);
    assert_eq!(chunks.last().unwrap().source_bytes.end, doc.text.len());
    let mut overlaps = 0;
    for pair in chunks.windows(2) {
        let a = &pair[0].source_bytes;
        let b = &pair[1].source_bytes;
        assert!(b.start <= a.end && b.end > a.end);
        if b.start < a.end {
            overlaps += 1;
            assert!(doc.text[b.start..a.end].chars().count() <= 50);
        }
    }
    assert!(overlaps > 0);
}
#[test]
fn code_lists_tables_and_enhanced_containers_remain_atomic() {
    for block in [
        format!("```rust\n{}\n```\n", "let x = 1;\n".repeat(30)),
        format!("- first\n{}", "  - nested item\n".repeat(30)),
        format!(
            "- first\n{}",
            "  <page>nested HTML source</page>\n".repeat(30)
        ),
        format!("|a|b|\n|-|-|\n{}", "|1|2|\n".repeat(30)),
        format!(
            "<details>\n\n# inner\n\n{}\n</details>\n",
            "paragraph\n\n".repeat(30)
        ),
    ] {
        let doc = document(&format!("# Outer\n\n{block}\n## Next\n\nend\n"));
        let chunks = chunk_document(
            &doc,
            ChunkConfig {
                target_chars: 50,
                overlap_chars: 5,
            },
        )
        .unwrap();
        assert!(chunks.iter().any(|c| c.text.contains(&block)), "{chunks:?}");
        assert_eq!(
            chunks.last().unwrap().metadata.heading_path,
            vec!["Outer", "Next"]
        );
    }
}
#[test]
fn setext_headings_inherited_path_empty_text_and_invalid_sizes() {
    let mut doc = document("Title\n=====\n\ntext\n");
    doc.metadata.heading_path = vec!["Inherited".into()];
    assert_eq!(
        chunk_document(&doc, ChunkConfig::default()).unwrap()[0]
            .metadata
            .heading_path,
        vec!["Inherited", "Title"]
    );
    assert!(
        chunk_document(&document(""), ChunkConfig::default())
            .unwrap()
            .is_empty()
    );
    for config in [
        ChunkConfig {
            target_chars: 0,
            overlap_chars: 0,
        },
        ChunkConfig {
            target_chars: 2,
            overlap_chars: 2,
        },
    ] {
        assert!(chunk_document(&doc, config).is_err());
    }
}

#[test]
fn prose_splits_cannot_manufacture_lists_headings_or_drop_hard_breaks() {
    for text in [
        "aaaa - item more prose",
        "aaaa + item more prose",
        "aaaa 1. item more prose",
        "aaaa 2) item more prose",
        "aaaa # heading more prose",
        "aaaa  \ncontinued prose",
        "aaaa  \r\ncontinued prose",
        "aaaa     indented prose",
        "aaaa \tindented prose",
    ] {
        let doc = document(text);
        let chunks = chunk_document(
            &doc,
            ChunkConfig {
                target_chars: 7,
                overlap_chars: 0,
            },
        )
        .unwrap();
        assert_eq!(chunks.len(), 1, "{text:?}: {chunks:?}");
        assert_eq!(chunks[0].text, text);
    }
}

#[test]
fn independent_complete_html_elements_split_without_changing_source_or_citations() {
    for tag in ["page", "database", "div", "details"] {
        let text = (0..30)
            .map(|n| {
                format!("<{tag} url=\"https://example.org/{n}\">Svensk åäö text {n}</{tag}>\n")
            })
            .collect::<String>();
        let doc = document(&text);
        let chunks = chunk_document(
            &doc,
            ChunkConfig {
                target_chars: 200,
                overlap_chars: 0,
            },
        )
        .unwrap();
        assert!(chunks.len() > 5);
        assert_eq!(
            chunks.iter().map(|c| c.text.as_str()).collect::<String>(),
            text
        );
        for chunk in chunks {
            assert!(chunk.text.chars().count() <= 200);
            assert_eq!(chunk.text, text[chunk.source_bytes]);
            assert_eq!(chunk.metadata, doc.metadata);
        }
    }
}

#[test]
fn html_pairing_preserves_nested_containers_and_ignores_quoted_or_commented_closes() {
    for block in [
        "<page>outer<page>nested</page>still outer</page>\n",
        "<page title=\"fake </page> close\">åäö</page>\n",
        "<page>before<!-- </page> -->after</page>\n",
        "<page><![CDATA[ </page> ]]></page>\n",
        "<script>const sample = '<page>';</script>\n",
        "<page title=\"fake </page>\nclose\">body</page>\n",
        "<PAGE>case-insensitive</page>\n",
    ] {
        let text = format!("{block}\n<page>independent sibling</page>\n\n# After\n\nend\n");
        let doc = document(&text);
        let chunks = chunk_document(
            &doc,
            ChunkConfig {
                target_chars: 12,
                overlap_chars: 0,
            },
        )
        .unwrap();
        assert!(
            chunks.iter().any(|c| c.text.contains(block)),
            "{block:?}: {chunks:?}"
        );
        assert!(
            !chunks
                .iter()
                .any(|c| c.text.contains(block) && c.text.contains("independent sibling")),
            "{chunks:?}"
        );
        assert_eq!(
            chunks.iter().map(|c| c.text.as_str()).collect::<String>(),
            text
        );
        assert_eq!(chunks.last().unwrap().metadata.heading_path, vec!["After"]);
    }
}

#[test]
fn literal_code_tags_do_not_own_later_source_and_unclosed_html_keeps_its_tail() {
    let text = "```html\n<page>unclosed code example\n```\n\n<page>real element</page>\n\n# After\n\nend\n";
    let chunks = chunk_document(
        &document(text),
        ChunkConfig {
            target_chars: 12,
            overlap_chars: 0,
        },
    )
    .unwrap();
    assert_eq!(chunks.last().unwrap().metadata.heading_path, vec!["After"]);
    assert!(
        !chunks
            .iter()
            .any(|c| c.text.contains("code example") && c.text.contains("real element"))
    );
    let text = "before\n\n<page>unclosed container\n\n# Not a separate section\n\ntail\n";
    let chunks = chunk_document(
        &document(text),
        ChunkConfig {
            target_chars: 12,
            overlap_chars: 0,
        },
    )
    .unwrap();
    assert!(
        chunks
            .iter()
            .any(|c| c.text.contains("<page>") && c.text.ends_with("tail\n"))
    );
    assert_eq!(
        chunks.iter().map(|c| c.text.as_str()).collect::<String>(),
        text
    );
}
