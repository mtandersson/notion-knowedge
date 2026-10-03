use notion_knowledge_core::{
    chunking::{ChunkConfig, chunk_document},
    fingerprint::{content_hash, identify_chunks},
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
fn run(
    text: &str,
    previous: &[notion_knowledge_core::indexed::IndexedChunk],
) -> Vec<notion_knowledge_core::indexed::IndexedChunk> {
    identify_chunks(
        chunk_document(&document(text), ChunkConfig::default()).unwrap(),
        previous,
    )
    .unwrap()
}
#[test]
fn logical_sections_survive_edits_insertions_removals_and_offset_changes() {
    let before = run(
        "# One\n\nOriginal\n\n# Two\n\nStable\n\n# Three\n\nRemoved\n",
        &[],
    );
    assert_eq!(
        before,
        run(
            "# One\n\nOriginal\n\n# Two\n\nStable\n\n# Three\n\nRemoved\n",
            &before
        )
    );
    let after = run(
        "# Added\n\nNew\n\n# One\n\nEdited\n\n# Two\n\nStable\n",
        &before,
    );
    assert_eq!(before[0].chunk_id, after[1].chunk_id);
    assert_ne!(before[0].content_hash, after[1].content_hash);
    assert_eq!(before[1].chunk_id, after[2].chunk_id);
    assert_eq!(before[1].content_hash, after[2].content_hash);
    assert!(!after.iter().any(|c| c.chunk_id == before[2].chunk_id));
    assert_ne!(after[0].chunk_id, after[1].chunk_id);
    assert_eq!(
        run("# Two\n\nStable\n", &[])[0].chunk_id,
        before[1].chunk_id
    );
}
#[test]
fn signed_credentials_rotate_without_hiding_semantic_url_changes() {
    for prefix in ["X-Amz", "X-Goog"] {
        let url = |signature: &str, filename: &str| {
            format!(
                "[Download](https://files.example/object?{prefix}-Signature={signature}&{prefix}-Credential=secret{signature}&{prefix}-Date={signature}&{prefix}-Expires={signature}&download={filename}#part)"
            )
        };
        assert_eq!(
            content_hash(&url("old", "report")),
            content_hash(&url("new", "report"))
        );
        assert_ne!(
            content_hash(&url("old", "report")),
            content_hash(&url("old", "other"))
        );
        assert_ne!(
            content_hash(&url("old", "report")),
            content_hash(&url("old", "report").replace("/object", "/other"))
        );
        let a = run(&format!("# File\n\n{}\n", url("old", "report")), &[]);
        let b = run(&format!("# File\n\n{}\n", url("new", "report")), &a);
        assert_eq!(a[0].chunk_id, b[0].chunk_id);
        assert_eq!(a[0].content_hash, b[0].content_hash);
        assert_ne!(a[0].text, b[0].text);
    }
    assert_ne!(
        content_hash("https://example.org/?token=a"),
        content_hash("https://example.org/?token=b")
    );
    assert_ne!(
        content_hash("https://example.org/?X-Amz-Date=a"),
        content_hash("https://example.org/?X-Amz-Date=b")
    );
    assert_ne!(
        content_hash("[first](https://example.org)"),
        content_hash("[second](https://example.org)")
    );
}
#[test]
fn fingerprints_preserve_searchable_structure_and_exclude_metadata() {
    assert_eq!(
        content_hash("hello"),
        "nk-content-v1:a4b18a7395714f5a7b7d52d42ac5427376b1fcbbb19142d4b2ad3ce34052f430"
    );
    assert_eq!(content_hash("å\r\n日本語\r\n"), content_hash("å\n日本語\n"));
    assert_ne!(content_hash("one  two"), content_hash("one two"));
    assert_ne!(content_hash("# Title\ntext"), content_hash("# Other\ntext"));
    let original = run("# Title\ntext", &[]);
    let mut doc = document("# Title\ntext");
    doc.metadata.last_edited_time = "another timestamp".into();
    let updated = identify_chunks(
        chunk_document(&doc, ChunkConfig::default()).unwrap(),
        &original,
    )
    .unwrap();
    assert_eq!(original[0].content_hash, updated[0].content_hash);
    assert_eq!(original[0].chunk_id, updated[0].chunk_id);
    doc.metadata.page_id = "other-page".into();
    assert_ne!(
        original[0].chunk_id,
        identify_chunks(chunk_document(&doc, ChunkConfig::default()).unwrap(), &[]).unwrap()[0]
            .chunk_id
    );
}
#[test]
fn duplicate_sections_keep_distinct_ids_and_exact_survivors() {
    let before = run("# Same\n\nA\n\n# Same\n\nB\n\n# Same\n\nA\n", &[]);
    assert_ne!(before[0].chunk_id, before[2].chunk_id);
    let after = run(
        "# Same\n\nInserted\n\n# Same\n\nB\n\n# Same\n\nA\n",
        &before,
    );
    assert_eq!(before[1].chunk_id, after[1].chunk_id);
    assert_eq!(before[0].chunk_id, after[2].chunk_id);
    assert_eq!(
        after
            .iter()
            .map(|c| &c.chunk_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        after.len()
    );
}
#[test]
fn rejects_invalid_snapshots_and_hashes_inherited_heading_context() {
    let old = run("body", &[]);
    let mut duplicate = old.clone();
    duplicate.extend(old.clone());
    assert!(identify_chunks(vec![], &duplicate).is_err());
    let mut tampered = old.clone();
    tampered[0].content_hash = "other-version:abc".into();
    assert!(identify_chunks(vec![], &tampered).is_err());
    let mut doc = document("body");
    doc.metadata.heading_path = vec!["Context".into()];
    let new = identify_chunks(chunk_document(&doc, ChunkConfig::default()).unwrap(), &old).unwrap();
    assert_ne!(old[0].content_hash, new[0].content_hash);
    assert_ne!(old[0].chunk_id, new[0].chunk_id);
}

#[test]
fn signed_url_semantic_query_encoding_is_preserved_verbatim() {
    for (left, right) in [("a+b", "a%20b"), ("%FF", "%FE")] {
        let url =
            |value| format!("https://files.example/path?X-Amz-Signature=old&download={value}");
        assert_ne!(content_hash(&url(left)), content_hash(&url(right)));
        assert_eq!(
            content_hash(&url(left)),
            content_hash(&url(left).replace("=old", "=new"))
        );
    }
}
