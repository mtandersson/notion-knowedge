//! Fresh, fail-closed preflight for future guarded Notion writes.
//!
//! This module deliberately does not mutate anything. The caller must authorize
//! the target before invoking it and must not treat a successful check as an
//! atomic Notion compare-and-swap: the source can change before a later PATCH.

use chrono::DateTime;
use sha2::{Digest, Sha256};

use crate::backend::{BackendError, BackendErrorKind, NotionRead, PageId};

/// A caller's revision, captured from an earlier authoritative read.
/// SHA-256 is over the **exact UTF-8 Markdown bytes**, not indexed/search text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExpectedRevision {
    pub last_edited_time: Option<String>,
    pub markdown_sha256: Option<String>,
}

/// Intentionally content-free metadata suitable for a conflict response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionMetadata {
    pub page_id: PageId,
    pub last_edited_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevisionError {
    /// Missing or malformed caller precondition; do not try a mutation.
    InvalidPrecondition,
    /// Source identity or upstream metadata was not trustworthy.
    Read(BackendError),
    /// A changed source revision requires an explicit fresh read and retry.
    Conflict(RevisionMetadata),
    /// An archived page must never be mutated through this guard.
    Inactive(RevisionMetadata),
}

fn internal_error() -> RevisionError {
    RevisionError::Read(BackendError {
        operation: "knowledge.revision_preflight",
        kind: BackendErrorKind::Internal,
        retry_after: None,
        committed_page_id: None,
    })
}

fn valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Compute the guard's exact Markdown hash for a previously fetched source.
/// Do not substitute the indexed normalized-content fingerprint.
pub fn markdown_sha256(markdown: &str) -> String {
    format!("{:x}", Sha256::digest(markdown.as_bytes()))
}

/// Notion may omit a single final LF when exporting persisted Markdown.
/// Only post-write verification uses this allowance; revision hashes stay exact.
///
/// Notion also normalises blank lines between blocks (it drops the one after a
/// heading, for example), so empty lines strictly between two non-empty lines
/// outside code fences are ignored. Block structure that only blank lines
/// express (loose versus tight lists, paragraph breaks) is therefore not verified. Leading and trailing blank lines, and every
/// line inside a fence, still have to match exactly.
pub fn written_markdown_matches(expected: &str, actual: &str) -> bool {
    expected == actual
        || expected.strip_suffix('\n') == Some(actual)
        || actual.strip_suffix('\n') == Some(expected)
        || without_inner_blank_lines(expected) == without_inner_blank_lines(actual)
}

fn without_inner_blank_lines(markdown: &str) -> Vec<&str> {
    let body = markdown.strip_suffix('\n').unwrap_or(markdown);
    let lines: Vec<&str> = body.split('\n').collect();
    let first = lines.iter().position(|line| !line.is_empty());
    let last = lines.iter().rposition(|line| !line.is_empty());
    let (Some(first), Some(last)) = (first, last) else {
        return lines;
    };
    // CommonMark fences: opened by 3+ backticks or tildes, closed only by the same
    // character, at least as many, and nothing else on the line.
    let mut open: Option<(char, usize)> = None;
    let mut kept: Vec<&str> = lines[..first].to_vec();
    for line in &lines[first..=last] {
        let trimmed = line.trim_start();
        let marker = trimmed
            .chars()
            .next()
            .filter(|c| matches!(c, '`' | '~'))
            .map(|c| (c, trimmed.chars().take_while(|x| *x == c).count()))
            .filter(|(_, run)| *run >= 3);
        let inside = open.is_some();
        match (open, marker) {
            (None, Some(found)) => open = Some(found),
            (Some((c, run)), Some((found_c, found_run)))
                if c == found_c && found_run >= run && trimmed[found_run..].trim().is_empty() =>
            {
                open = None;
            }
            _ => {}
        }
        if inside || open.is_some() || !line.is_empty() {
            kept.push(line);
        }
    }
    kept.extend_from_slice(&lines[last + 1..]);
    kept
}

/// Compare caller preconditions against one fresh authoritative page read.
///
/// All supplied conditions must match. No local index is consulted, no
/// mutation is attempted, and no automatic retry is performed. After a
/// conflict, the caller must fetch current content and explicitly retry.
pub async fn check_revision(
    reader: &dyn NotionRead,
    page_id: &PageId,
    expected: &ExpectedRevision,
) -> Result<RevisionMetadata, RevisionError> {
    if expected.last_edited_time.is_none() && expected.markdown_sha256.is_none() {
        return Err(RevisionError::InvalidPrecondition);
    }
    let expected_time = expected
        .last_edited_time
        .as_deref()
        .map(DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| RevisionError::InvalidPrecondition)?;
    if expected
        .markdown_sha256
        .as_deref()
        .is_some_and(|hash| !valid_hash(hash))
    {
        return Err(RevisionError::InvalidPrecondition);
    }

    // A hash requires full fresh Markdown. Otherwise fetch only metadata.
    let (page, current_hash) = if expected.markdown_sha256.is_some() {
        let content = reader
            .read_content(page_id)
            .await
            .map_err(RevisionError::Read)?;
        let hash = markdown_sha256(&content.markdown);
        (content.page, Some(hash))
    } else {
        (
            reader
                .fetch_page(page_id)
                .await
                .map_err(RevisionError::Read)?,
            None,
        )
    };
    if &page.id != page_id {
        return Err(internal_error());
    }
    let current_time =
        DateTime::parse_from_rfc3339(&page.last_edited_time).map_err(|_| internal_error())?;
    let metadata = RevisionMetadata {
        page_id: page.id,
        last_edited_time: page.last_edited_time,
    };
    if page.archived {
        return Err(RevisionError::Inactive(metadata));
    }
    let hash_changed = match (expected.markdown_sha256.as_deref(), current_hash.as_deref()) {
        (Some(expected_hash), Some(actual_hash)) => {
            !expected_hash.eq_ignore_ascii_case(actual_hash)
        }
        (None, _) => false,
        _ => return Err(internal_error()),
    };
    if expected_time.is_some_and(|time| time != current_time) || hash_changed {
        return Err(RevisionError::Conflict(metadata));
    }
    Ok(metadata)
}

/// A second fresh read must be evaluated against the exact same user
/// preconditions immediately before PATCH. No implicit update or replay.
pub fn compare_content_snapshot(
    page_id: &PageId,
    expected: &ExpectedRevision,
    content: &crate::backend::PageContent,
) -> Result<RevisionMetadata, RevisionError> {
    if expected.last_edited_time.is_none() && expected.markdown_sha256.is_none() {
        return Err(RevisionError::InvalidPrecondition);
    }
    let time = expected
        .last_edited_time
        .as_deref()
        .map(DateTime::parse_from_rfc3339)
        .transpose()
        .map_err(|_| RevisionError::InvalidPrecondition)?;
    if expected
        .markdown_sha256
        .as_deref()
        .is_some_and(|h| !valid_hash(h))
    {
        return Err(RevisionError::InvalidPrecondition);
    }
    if &content.page.id != page_id {
        return Err(internal_error());
    }
    let current = DateTime::parse_from_rfc3339(&content.page.last_edited_time)
        .map_err(|_| internal_error())?;
    let meta = RevisionMetadata {
        page_id: content.page.id.clone(),
        last_edited_time: content.page.last_edited_time.clone(),
    };
    if content.page.archived {
        return Err(RevisionError::Inactive(meta));
    }
    if time.is_some_and(|t| t != current)
        || expected
            .markdown_sha256
            .as_deref()
            .is_some_and(|h| !h.eq_ignore_ascii_case(&markdown_sha256(&content.markdown)))
    {
        return Err(RevisionError::Conflict(meta));
    }
    Ok(meta)
}

#[cfg(test)]
mod written_markdown_contract {
    use super::written_markdown_matches;

    #[test]
    fn only_one_terminal_line_feed_may_differ() {
        for (expected, actual, matches) in [
            ("paragraph\n", "paragraph", true),
            ("paragraph", "paragraph\n", true),
            ("paragraph\n", "paragraph\n", true),
            ("paragraph\n\n", "paragraph", false),
            ("one\ntwo\n", "onetwo", false),
            ("    code\n", "code", false),
            ("paragraph  \n", "paragraph", false),
            ("paragraph\r\n", "paragraph", false),
        ] {
            assert_eq!(written_markdown_matches(expected, actual), matches);
        }
    }

    #[test]
    fn blank_lines_between_blocks_may_differ_but_never_inside_code() {
        for (expected, actual, matches) in [
            (
                "## Smaker\n\n- sött\n- surt",
                "## Smaker\n- sött\n- surt",
                true,
            ),
            ("## Smaker\n- sött\n", "## Smaker\n\n- sött", true),
            ("one\n\n\ntwo", "one\ntwo", true),
            ("one\n\ntwo", "one two", false),
            ("one\n\ntwo", "one\ntwo\nthree", false),
            ("```\na\n\nb\n```", "```\na\nb\n```", false),
            ("```\na\n\nb\n```", "```\na\n\nb\n```\n", true),
            ("one\n\n", "one", false),
            ("\n\none", "one", false),
        ] {
            assert_eq!(
                written_markdown_matches(expected, actual),
                matches,
                "{expected:?} vs {actual:?}"
            );
        }
    }

    /// A ChatGPT row body and the Markdown Notion persisted for it (read back
    /// from the API): Notion dropped every blank line between blocks.
    #[test]
    fn fences_are_tracked_by_character_length_and_closing_rules() {
        for (expected, actual, matches) in [
            // A backtick line does not close a tilde fence.
            (
                "a\n\n~~~\nx\n```\n\ny\n~~~",
                "a\n~~~\nx\n```\ny\n~~~",
                false,
            ),
            (
                "a\n\n~~~\nx\n```\n\ny\n~~~",
                "a\n~~~\nx\n```\n\ny\n~~~",
                true,
            ),
            // A shorter run does not close a longer fence.
            ("````\nx\n```\n\ny\n````", "````\nx\n```\ny\n````", false),
            // An info string opens a fence; text after the marker cannot close one.
            ("```rust\nx\n\ny\n```", "```rust\nx\ny\n```", false),
            (
                "```\nx\n``` not a close\n\ny\n```",
                "```\nx\n``` not a close\ny\n```",
                false,
            ),
            // Blank lines after a closed fence are ordinary again.
            ("```\nx\n```\n\ntext", "```\nx\n```\ntext", true),
            // An unclosed fence keeps everything after it strict.
            ("```\nx\n\ny", "```\nx\ny", false),
        ] {
            assert_eq!(
                written_markdown_matches(expected, actual),
                matches,
                "{expected:?} vs {actual:?}"
            );
        }
    }

    #[test]
    fn notion_persisted_chatgpt_markdown_still_verifies() {
        let sent = "## Kännetecken\nThailändsk matlagning balanserar kontrasterande smaker, färska örter, kryddor och aromatiska ingredienser. Regionala variationer är stora.\n\n## Fem centrala smakdimensioner\n- Sött – palmsocker.\n- Surt – lime och tamarind.\n- Salt – fisksås.\n- Beskt – exempelvis bittermelon och vissa örter.\n- Starkt – chili.\n\nChilihetta är en sensorisk upplevelse snarare än en biologisk grundsmak. Umami från exempelvis fisksås och räkpasta bidrar också till smakdjupet.\n\n## Tre klassiska rätter\n- Pad Thai – wokade risnudlar med tamarind, ägg, böngroddar och jordnötter, ofta med räkor eller tofu.\n- Tom Yum Goong – het och syrlig räksoppa med citrongräs, galangal och makrutlimeblad.\n- Gaeng Khiao Wan – grön curry med grön chilipasta, kokosmjölk och thaibasilika, ofta med kyckling och aubergine.\n\n## Källor\n- [Thailands honorärkonsulat i Kraków – regional matkultur](https://konsulat-tajlandii-krakow.pl/en/thai-cuisine-a-journey-through-four-regions-of-flavour/)\n- [Southeast Asia Travel – Dining in Thailand](https://staging.visitsoutheastasia.travel/guides/dining-in-thailand/)\n- [Thailand Insider Guide – Thai Food Guide](https://thailandinsiderguide.com/en/explore-thailand/experiences/food-drinks/thai-food-guide/)\n\nUndersökt 2026-11-26.";
        let stored = "## Kännetecken\nThailändsk matlagning balanserar kontrasterande smaker, färska örter, kryddor och aromatiska ingredienser. Regionala variationer är stora.\n## Fem centrala smakdimensioner\n- Sött – palmsocker.\n- Surt – lime och tamarind.\n- Salt – fisksås.\n- Beskt – exempelvis bittermelon och vissa örter.\n- Starkt – chili.\nChilihetta är en sensorisk upplevelse snarare än en biologisk grundsmak. Umami från exempelvis fisksås och räkpasta bidrar också till smakdjupet.\n## Tre klassiska rätter\n- Pad Thai – wokade risnudlar med tamarind, ägg, böngroddar och jordnötter, ofta med räkor eller tofu.\n- Tom Yum Goong – het och syrlig räksoppa med citrongräs, galangal och makrutlimeblad.\n- Gaeng Khiao Wan – grön curry med grön chilipasta, kokosmjölk och thaibasilika, ofta med kyckling och aubergine.\n## Källor\n- [Thailands honorärkonsulat i Kraków – regional matkultur](https://konsulat-tajlandii-krakow.pl/en/thai-cuisine-a-journey-through-four-regions-of-flavour/)\n- [Southeast Asia Travel – Dining in Thailand](https://staging.visitsoutheastasia.travel/guides/dining-in-thailand/)\n- [Thailand Insider Guide – Thai Food Guide](https://thailandinsiderguide.com/en/explore-thailand/experiences/food-drinks/thai-food-guide/)\nUndersökt 2026-11-26.";
        assert!(written_markdown_matches(sent, stored));
        assert!(!written_markdown_matches(
            sent,
            &stored.replace("chili.", "paprika.")
        ));
    }
}
