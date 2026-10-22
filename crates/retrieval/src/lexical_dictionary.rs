//! Literal dictionary planning only. No network, corpus, judgments or search calls.
//!
//! Construction is private to this module: the offline importer will produce a
//! verified immutable dictionary. Runtime installation is a separate contract.
#[cfg(test)]
mod import;

use std::collections::BTreeSet;
use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};
use unicode_segmentation::UnicodeSegmentation;

const MAX_POSITIONS: usize = 128;
const MAX_UNITS: usize = 8;
const MAX_TARGETS: usize = 4;
const MAX_PHRASES: usize = 8;

/// Versioned semantics; identity also binds source, lockfile and Unicode tables.
pub const PLANNER_VERSION: &str = "literal-en-sv-query-expansion-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Edge {
    source: Box<str>,
    target: Box<str>,
    /// One-based ordinal in the public source, never a private content ID.
    entry: u32,
}

/// Immutable sorted literal relations. Loading/construction is not a tool input.
#[derive(Debug)]
pub struct Dictionary {
    edges: Box<[Edge]>,
}

/// Public-asset provenance; generated text is for the native literal FTS executor.
/// Do not log or expose this structure via diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct Phrase {
    text: Box<str>,
    entry: u32,
    ordinal: u8,
}
impl Phrase {
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn entry_ordinal(&self) -> u32 {
        self.entry
    }
    pub fn planner_ordinal(&self) -> u8 {
        self.ordinal
    }
}

/// Bounded counters contain no original or generated query strings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    pub examined_positions: u16,
    pub matched_units: u8,
    pub ambiguous_units: u8,
    pub unmatched_positions: u16,
    pub position_limit: bool,
    pub unit_limit: bool,
    pub phrase_limit: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bypass {
    None,
    QuotedPhrase,
    ExactIdentifier,
}

pub struct Plan {
    phrases: Vec<Phrase>,
    pub counters: Counters,
    pub bypass: Bypass,
}
impl Plan {
    pub fn phrases(&self) -> &[Phrase] {
        &self.phrases
    }
}

fn normalized(text: &str) -> String {
    text.nfc().flat_map(char::to_lowercase).nfc().collect()
}

/// Reject punctuation/operators rather than turning them into unrelated words.
fn literal(text: &str) -> Option<String> {
    let normalized = normalized(text);
    if normalized
        .chars()
        .any(|c| !c.is_alphabetic() && !is_combining_mark(c) && !c.is_whitespace())
    {
        return None;
    }
    let words: Vec<_> = normalized.unicode_words().collect();
    if !(1..=4).contains(&words.len()) || words.iter().any(|w| w.chars().count() > 64) {
        return None;
    }
    Some(words.join(" "))
}

impl Dictionary {
    /// The owning adapter must recognize exact stable IDs against its validated
    /// query/ID contract before calling; this boolean is never client policy.
    /// Lookup scans at most 128 positions; no recursive translation is performed.
    pub fn plan(&self, query: &str, exact_identifier: bool) -> Plan {
        let trimmed = query.trim();
        let quoted = trimmed
            .strip_prefix('"')
            .and_then(|q| q.strip_suffix('"'))
            .is_some_and(|q| !q.trim().is_empty() && !q.contains('"'));
        let bypass = if exact_identifier {
            Bypass::ExactIdentifier
        } else if quoted {
            Bypass::QuotedPhrase
        } else {
            Bypass::None
        };
        let mut plan = Plan {
            phrases: Vec::new(),
            counters: Counters::default(),
            bypass,
        };
        if bypass != Bypass::None {
            return plan;
        }
        // Existing query validation bounds this to 4096 scalars. Keep this pure
        // component bounded too, including normalization of direct callers.
        if query.chars().take(4097).count() > 4096 {
            plan.counters.position_limit = true;
            return plan;
        }
        let copy = normalized(query);
        let all: Vec<_> = copy
            .unicode_word_indices()
            .take(MAX_POSITIONS + 1)
            .collect();
        plan.counters.position_limit = all.len() > MAX_POSITIONS;
        let words = &all[..all.len().min(MAX_POSITIONS)];
        let mut units: Vec<Vec<&Edge>> = Vec::new();
        let mut position = 0;
        while position < words.len() {
            if units.len() == MAX_UNITS {
                plan.counters.unit_limit = true;
                break;
            }
            let mut matched = None;
            for count in (1..=4.min(words.len() - position)).rev() {
                let start = words[position].0;
                let (end, word) = words[position + count - 1];
                let Some(unit) = literal(&copy[start..end + word.len()]) else {
                    continue;
                };
                let first = self
                    .edges
                    .partition_point(|edge| edge.source.as_ref() < unit.as_str());
                let last = self
                    .edges
                    .partition_point(|edge| edge.source.as_ref() <= unit.as_str());
                if first != last {
                    matched = Some((count, &self.edges[first..last]));
                    break;
                }
            }
            if let Some((count, edges)) = matched {
                position += count;
                // Source/target canonicalization leaves one edge per relation.
                if edges.len() > MAX_TARGETS {
                    plan.counters.ambiguous_units = plan.counters.ambiguous_units.saturating_add(1);
                } else {
                    units.push(edges.iter().collect());
                    plan.counters.matched_units += 1;
                }
            } else {
                position += 1;
                plan.counters.unmatched_positions += 1;
            }
        }
        plan.counters.examined_positions = position as u16;
        let mut seen = BTreeSet::new();
        for target_index in 0..MAX_TARGETS {
            for unit in &units {
                if let Some(edge) = unit.get(target_index) {
                    if seen.insert(edge.target.as_ref()) {
                        if plan.phrases.len() == MAX_PHRASES {
                            plan.counters.phrase_limit = true;
                            continue;
                        }
                        plan.phrases.push(Phrase {
                            text: edge.target.clone(),
                            entry: edge.entry,
                            ordinal: plan.phrases.len() as u8,
                        });
                    }
                }
            }
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dictionary(pairs: &[(&str, &str)]) -> Dictionary {
        let mut edges = Vec::new();
        for (i, (source, target)) in pairs.iter().enumerate() {
            let source = literal(source).unwrap();
            let target = literal(target).unwrap();
            edges.push(Edge {
                source: source.clone().into(),
                target: target.clone().into(),
                entry: i as u32 + 1,
            });
            edges.push(Edge {
                source: target.into(),
                target: source.into(),
                entry: i as u32 + 1,
            });
        }
        edges.sort_by(|a, b| (&a.source, &a.target, a.entry).cmp(&(&b.source, &b.target, b.entry)));
        edges.dedup_by(|a, b| a.source == b.source && a.target == b.target);
        Dictionary {
            edges: edges.into_boxed_slice(),
        }
    }
    fn texts(plan: &Plan) -> Vec<&str> {
        plan.phrases().iter().map(Phrase::text).collect()
    }
    #[test]
    fn literal_bidirectional_lookup_normalizes_without_recursive_synonyms() {
        let dictionary = dictionary(&[("island", "ö"), ("river", "flod"), ("stream", "flod")]);
        assert_eq!(texts(&dictionary.plan("O\u{308}", false)), ["island"]);
        assert_eq!(texts(&dictionary.plan("RIVER", false)), ["flod"]);
        assert_eq!(texts(&dictionary.plan("flod", false)), ["river", "stream"]);
        assert!(dictionary.plan("rivers unknown", false).phrases.is_empty());
        assert!(dictionary.plan("島", false).phrases.is_empty());
    }
    #[test]
    fn longest_nonoverlapping_units_preserve_punctuation_boundaries() {
        let dictionary = dictionary(&[("ice cream", "glass"), ("ice", "is"), ("cream", "grädde")]);
        assert_eq!(
            texts(&dictionary.plan("ice cream ice", false)),
            ["glass", "is"]
        );
        assert_eq!(
            texts(&dictionary.plan("ice, cream", false)),
            ["is", "grädde"]
        );
        assert!(literal("a OR(b)").is_none());
        assert!(literal("a-b").is_none());
        assert!(literal(&"a".repeat(65)).is_none());
    }
    #[test]
    fn quoted_queries_and_exact_identifiers_do_not_expand() {
        let dictionary = dictionary(&[("island", "ö")]);
        let quoted = dictionary.plan(" \"island\" ", false);
        assert_eq!(quoted.bypass, Bypass::QuotedPhrase);
        assert!(quoted.phrases.is_empty());
        let exact = dictionary.plan("island", true);
        assert_eq!(exact.bypass, Bypass::ExactIdentifier);
        assert!(exact.phrases.is_empty());
    }
    #[test]
    fn ambiguous_units_are_skipped_and_round_robin_is_deterministic() {
        let dictionary = dictionary(&[
            ("bank", "strand"),
            ("bank", "bänk"),
            ("bank", "bank"),
            ("bank", "vall"),
            ("bank", "lager"),
            ("light", "ljus"),
            ("light", "lätt"),
            ("dark", "mörk"),
            ("dark", "dyster"),
        ]);
        let plan = dictionary.plan("bank light dark", false);
        assert_eq!(plan.counters.ambiguous_units, 1);
        assert_eq!(texts(&plan), ["ljus", "dyster", "lätt", "mörk"]);
        assert_eq!(
            texts(&plan),
            texts(&dictionary.plan("bank light dark", false))
        );
    }
    #[test]
    fn lookup_and_output_caps_are_explicit_without_query_text() {
        let pairs: Vec<_> = (0..10)
            .map(|i| {
                (
                    format!("word{}", char::from(b'a' + i)),
                    format!("term{}", char::from(b'a' + i)),
                )
            })
            .collect();
        let refs: Vec<_> = pairs
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        let dictionary = dictionary(&refs);
        let query = pairs
            .iter()
            .map(|p| p.0.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let plan = dictionary.plan(&query, false);
        assert_eq!(plan.phrases.len(), 8);
        assert!(plan.counters.unit_limit);
        let plan = dictionary.plan(&"unknown ".repeat(129), false);
        assert!(plan.counters.position_limit);
        assert_eq!(plan.counters.examined_positions, 128);
    }
}
