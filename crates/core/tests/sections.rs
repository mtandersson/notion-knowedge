use notion_knowledge_core::sections::{SectionAnchor, SectionError, SectionPlan, plan_section};

fn at(level: u8, heading: &str) -> SectionAnchor {
    SectionAnchor {
        level,
        heading: heading.into(),
    }
}

fn plan(markdown: &str, level: u8, heading: &str, new_body: &str) -> SectionPlan {
    plan_section(markdown, at(level, heading), new_body).unwrap()
}

#[test]
fn replaces_only_the_exact_section_body_and_preserves_higher_siblings() {
    let original = "# Document\nIntro\n## Target\nOld paragraph\n### Child\n- old detail\n## Sibling\nKeep *all* of this\n# Footer\nEnd\n";
    let replacement = "New paragraph\n\n### New child\nnew detail\n\n";
    let edit = plan(original, 2, "Target", replacement);
    let proposed = "# Document\nIntro\n## Target\nNew paragraph\n\n### New child\nnew detail\n\n## Sibling\nKeep *all* of this\n# Footer\nEnd\n";
    assert_eq!(edit.preview(), proposed);
    assert!(edit.verify_readback(proposed));
    assert_eq!(edit.original_body(), "Old paragraph\n### Child\n- old detail\n");
    assert_eq!(edit.replacement_body(), replacement);
    assert!(edit.untouched_prefix().ends_with("## Target\n"));
    assert!(edit.untouched_suffix().starts_with("## Sibling\n"));
    assert_eq!(&original[edit.body_range.clone()], edit.original_body());
    assert_eq!(edit.original_sha256.len(), 64);
    assert_eq!(edit.proposed_sha256.len(), 64);
    assert_ne!(edit.original_sha256, edit.proposed_sha256);
}

#[test]
fn duplicate_literal_heading_is_rejected_instead_of_picking_first() {
    let content = "## Notes\nfirst\n# Other\n## Notes\nsecond\n";
    assert_eq!(
        plan_section(content, at(2, "Notes"), "replacement\n").unwrap_err(),
        SectionError::Ambiguous
    );
    let content = "## Notes\n## Notes\n";
    assert_eq!(
        plan_section(content, at(2, "Notes"), "").unwrap_err(),
        SectionError::Ambiguous
    );
}

#[test]
fn missing_or_partial_heading_is_never_a_match() {
    for (content, name) in [
        ("## Vacation plans\ntext\n", "Vacation"),
        ("## Vacation plan\ntext\n", "Vacation plans"),
        ("## Notes\n", "notes"),
        ("### Notes\n", "Other"),
    ] {
        assert_eq!(
            plan_section(content, at(2, name), "new\n").unwrap_err(),
            SectionError::NotFound
        );
    }
}

#[test]
fn heading_level_disambiguates_same_text_at_different_depths() {
    let source = "# Todo\nroot body\n## Todo\nchild body\n# End\n";
    let p = plan(source, 2, "Todo", "replacement\n");
    assert_eq!(
        p.preview(),
        "# Todo\nroot body\n## Todo\nreplacement\n# End\n"
    );
    let p = plan(source, 1, "Todo", "updated parent\n");
    assert_eq!(p.preview(), "# Todo\nupdated parent\n# End\n");
}

#[test]
fn fenced_pseudorubrics_and_inline_text_do_not_count_as_anchors() {
    let original = "~~~md\n## Target\n~~~\n\n\`## Target\`\n## Target\nreal body\n# End\n";
    let p = plan(original, 2, "Target", "new body\n");
    assert_eq!(
        p.preview(),
        "~~~md\n## Target\n~~~\n\n\`## Target\`\n## Target\nnew body\n# End\n"
    );
    assert_eq!(
        plan_section("~~~\n## Target\n~~~\n", at(2, "Target"), "new\n").unwrap_err(),
        SectionError::NotFound
    );
}

#[test]
fn blockquoted_and_indented_headings_are_not_targets_or_sibling_boundaries() {
    let source = "## Target\nBefore\n> ## Note\n> quoted text\n\n## Next\nafter\n";
    let p = plan(source, 2, "Target", "Replacement\n");
    assert_eq!(p.original_body(), "Before\n> ## Note\n> quoted text\n\n");
    assert_eq!(p.untouched_suffix(), "## Next\nafter\n");
    assert_eq!(
        plan_section("> ## Target\nquoted text\n", at(2, "Target"), "new\n").unwrap_err(),
        SectionError::NotFound
    );
    assert_eq!(
        plan_section("  - item\n    ## Target\n    detail\n", at(2, "Target"), "new\n").unwrap_err(),
        SectionError::NotFound
    );
}

#[test]
fn unicode_and_crlf_source_are_byte_exact() {
    let original = "Preamble 😊\r\n## Räksmörgås 🦐\r\nHallå 世界\r\n\r\n## Efteråt\r\nÄndra inte\r\n";
    let edit = plan(original, 2, "Räksmörgås 🦐", "Nytt 🦊\r\n");
    assert_eq!(
        edit.preview(),
        "Preamble 😊\r\n## Räksmörgås 🦐\r\nNytt 🦊\r\n## Efteråt\r\nÄndra inte\r\n"
    );
    assert!(edit.verify_readback(&edit.preview()));
    assert_eq!(&original[edit.body_range.clone()], "Hallå 世界\r\n\r\n");
    assert_eq!(edit.preview().as_bytes()[..edit.untouched_prefix().len()],
        *edit.untouched_prefix().as_bytes());
}

#[test]
fn empty_body_is_valid_and_explicit_empty_replacement_removes_only_body() {
    let original = "## Empty\n## Next\nretained\n";
    let p = plan(original, 2, "Empty", "new\n");
    assert_eq!(p.original_body(), "");
    assert_eq!(p.preview(), "## Empty\nnew\n## Next\nretained\n");

    let p = plan("## Target\nold\n## Next\n", 2, "Target", "");
    assert_eq!(p.preview(), "## Target\n## Next\n");
    assert!(p.verify_readback(&p.preview()));
}

#[test]
fn missing_terminal_newline_is_allowed_for_end_of_page_body_not_the_heading() {
    let p = plan("## Target\nold", 2, "Target", "new");
    assert_eq!(p.preview(), "## Target\nnew");
    assert_eq!(p.original_body(), "old");
    assert_eq!(
        plan_section("## Target", at(2, "Target"), "new").unwrap_err(),
        SectionError::Unsupported
    );
}

#[test]
fn no_op_is_explicitly_rejected() {
    assert_eq!(
        plan_section("## Target\nsame\n", at(2, "Target"), "same\n").unwrap_err(),
        SectionError::NoChange
    );
}

#[test]
fn replacement_must_not_join_a_sibling_heading_into_its_last_line() {
    let original = "## Target\nold\n## Sibling\nkeep\n";
    assert_eq!(
        plan_section(original, at(2, "Target"), "missing newline").unwrap_err(),
        SectionError::InvalidInput
    );
    let p = plan(original, 2, "Target", "new\n");
    assert!(p.preview().contains("new\n## Sibling\nkeep"));
}

#[test]
fn unsupported_enhanced_notion_markup_in_selected_body_is_denied() {
    for marker in [
        "<unknown url=\"https://example.org\"/>",
        "<page url=\"https://notion.so/foo\">",
        "<database id=\"anything\">",
        "<data-source id=\"anything\">",
    ] {
        let original = format!("## Target\n{marker}\n## Next\nkeep\n");
        assert_eq!(
            plan_section(&original, at(2, "Target"), "replacement\n").unwrap_err(),
            SectionError::Unsupported
        );
    }
}

#[test]
fn unaffected_enhanced_markup_is_not_rewritten() {
    let original = "# Page\n<unknown alt=\"keep\"/>\n## Target\nold\n## Next\n<page url=\"keep\"/>\n";
    let p = plan(original, 2, "Target", "new\n");
    assert_eq!(p.preview(), "# Page\n<unknown alt=\"keep\"/>\n## Target\nnew\n## Next\n<page url=\"keep\"/>\n");
}

#[test]
fn headings_containing_inline_markup_are_not_guessed() {
    for literal in ["## **Important**\ntext\n", "## [Important](https://example.com)\n", "Important\n---------\n"] {
        assert_eq!(
            plan_section(literal, at(2, "Important"), "new\n").unwrap_err(),
            SectionError::NotFound
        );
    }
}

#[test]
fn setext_heading_bounds_an_existing_atx_section() {
    let original = "## Target\nold\nAnother title\n=============\nsafe\n";
    let p = plan(original, 2, "Target", "new\n");
    assert_eq!(p.preview(), "## Target\nnew\nAnother title\n=============\nsafe\n");
}

#[test]
fn parser_does_not_treat_quoted_higher_level_heading_as_section_end() {
    let original = "## Target\nparagraph\n> # Quoted heading\n> content\n## Next\nuntouched\n";
    let p = plan(original, 2, "Target", "replacement\n");
    assert!(p.original_body().contains("> # Quoted heading\n> content"));
    assert_eq!(p.untouched_suffix(), "## Next\nuntouched\n");
}

#[test]
fn invalid_anchor_or_payload_is_rejected_before_processing() {
    let source = "## Target\nold\n";
    for anchor in [
        at(0, "Target"),
        at(7, "Target"),
        at(2, ""),
        at(2, " Target"),
        at(2, "Target "),
        at(2, "Target\nNext"),
        at(2, "Target\0"),
        at(2, "<tag>"),
        at(2, "`code`"),
        at(2, "x".repeat(201).as_str()),
    ] {
        assert_eq!(
            plan_section(source, anchor, "new\n").unwrap_err(),
            SectionError::InvalidInput
        );
    }
    assert_eq!(
        plan_section(source, at(2, "Target"), "bad\0content").unwrap_err(),
        SectionError::InvalidInput
    );
}

#[test]
fn over_limit_markdown_and_replacement_are_rejected() {
    let source = format!("## Target\n{}", "x".repeat(1024 * 1024));
    assert_eq!(
        plan_section(&source, at(2, "Target"), "replacement").unwrap_err(),
        SectionError::InvalidInput
    );
    assert_eq!(
        plan_section("## Target\nold\n", at(2, "Target"), &"x".repeat(200_001)).unwrap_err(),
        SectionError::InvalidInput
    );
}

#[test]
fn readback_detects_modifications_to_both_neighbors_and_wrong_replacement() {
    let original = "# Title\nintroduction\n## Target\nold\n## Other\nkeep\n";
    let p = plan(original, 2, "Target", "new\n");
    assert!(p.verify_readback(&p.preview()));
    assert!(!p.verify_readback(original));
    assert!(!p.verify_readback("# Title\nchanged\n## Target\nnew\n## Other\nkeep\n"));
    assert!(!p.verify_readback("# Title\nintroduction\n## Target\nnew\n## Other\nchanged\n"));
    assert!(!p.verify_readback("# Title\nintroduction\n## Target\nwrong\n## Other\nkeep\n"));
}

#[test]
fn trailing_lower_level_heading_belongs_to_selected_parent() {
    let original = "# Parent\nText\n## Child\nchild body\n### Grandchild\nchild detail\n";
    let p = plan(original, 1, "Parent", "parent rewritten\n");
    assert_eq!(p.preview(), "# Parent\nparent rewritten\n");
    assert!(p.untouched_suffix().is_empty());
}
