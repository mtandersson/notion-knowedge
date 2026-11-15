//! Pair only markup emitted by the Markdown parser, preserving source offsets.
use std::ops::Range;

#[derive(Default)]
pub(super) struct HtmlSpans {
    open: Vec<(String, usize, bool)>,
    pending: Option<Markup>,
    spans: Vec<Range<usize>>,
    boundaries: Vec<Range<usize>>,
}

struct Markup {
    start: usize,
    text: String,
    quote: Option<char>,
    standalone: bool,
}

impl HtmlSpans {
    pub(super) fn observe(&mut self, fragment: &str, offset: usize, standalone: bool) {
        for (index, ch) in fragment.char_indices() {
            let raw_text = self.open.last().is_some_and(|(tag, _, _)| {
                matches!(tag.as_str(), "script" | "style" | "textarea" | "title")
            });
            // Raw-text contents are not HTML attributes/comments. A bare '<'
            // or quote in that text must not swallow the real closing tag.
            if ch == '<' && (self.pending.is_none() || raw_text) {
                self.pending = Some(Markup {
                    start: offset + index,
                    text: "<".into(),
                    quote: None,
                    standalone,
                });
                continue;
            }
            if self.pending.is_none() {
                continue;
            }
            let markup = self.pending.as_mut().expect("pending markup");
            if (markup.text == "<" && !ch.is_ascii_alphabetic() && !matches!(ch, '/' | '!' | '?'))
                || (markup.text == "</" && !ch.is_ascii_alphabetic())
            {
                // Comparisons and literal less-than signs in an HTML block
                // are not incomplete tags. Keep scanning for real markup.
                self.pending = (ch == '<').then(|| Markup {
                    start: offset + index,
                    text: "<".into(),
                    quote: None,
                    standalone,
                });
                continue;
            }
            markup.text.push(ch);
            let finished = if markup.text.starts_with("<!--") {
                markup.text.ends_with("-->")
            } else if markup.text.starts_with("<![CDATA[") {
                markup.text.ends_with("]]>")
            } else if let Some(quote) = markup.quote {
                if ch == quote {
                    markup.quote = None;
                }
                false
            } else if matches!(ch, '\'' | '"') {
                markup.quote = Some(ch);
                false
            } else {
                ch == '>'
            };
            if finished {
                let markup = self.pending.take().expect("finished markup");
                self.complete(markup, offset + index + ch.len_utf8());
            }
        }
    }

    fn complete(&mut self, markup: Markup, end: usize) {
        let Some(body) = markup.text.strip_prefix('<') else {
            return;
        };
        let closing = body.starts_with('/');
        let body = body.strip_prefix('/').unwrap_or(body);
        let name: String = body
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '-')
            .collect();
        if name.is_empty() {
            return; // Comments, declarations and processing instructions.
        }
        let name = name.to_ascii_lowercase();
        // These HTML elements contain raw text; apparent tags inside are not
        // sibling containers. Only their own closing token ends the element.
        if self.open.last().is_some_and(|(tag, _, _)| {
            matches!(tag.as_str(), "script" | "style" | "textarea" | "title")
                && (!closing || tag != &name)
        }) {
            return;
        }
        if closing {
            if self.open.last().is_some_and(|(tag, _, _)| tag == &name) {
                let (_, start, standalone) = self.open.pop().expect("matching open element");
                self.spans.push(start..end);
                if standalone {
                    self.boundaries.push(start..end);
                }
            }
        } else if !markup.text.trim_end().ends_with("/>")
            && ![
                "br", "hr", "img", "input", "meta", "link", "wbr", "area", "base", "embed",
                "source", "track", "col", "param",
            ]
            .contains(&name.as_str())
        {
            self.open.push((name, markup.start, markup.standalone));
        }
    }

    pub(super) fn finish(mut self, source_end: usize) -> (Vec<Range<usize>>, Vec<Range<usize>>) {
        // Malformed/unclosed containers remain conservative atomic tails.
        self.spans
            .extend(self.open.into_iter().map(|(_, start, _)| start..source_end));
        if let Some(markup) = self.pending {
            self.spans.push(markup.start..source_end);
        }
        (self.spans, self.boundaries)
    }
}
