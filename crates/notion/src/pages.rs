//! Exact authoritative page metadata reads. No content or credential logging.
use crate::NotionClient;
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, Page, PageId},
    indexed::PropertyValue,
};
use reqwest::{Url, header::AUTHORIZATION};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const PAGE_VERSION: &str = "2026-03-11";
/// Marks a relation, people, title or rich-text value whose items were read in full through the
/// property item endpoint. Never present in a Notion response key we keep.
const PAGINATED: &str = "__nk_paginated";
/// Items read for one truncated property before the page is unsupported.
const MAX_PROPERTY_ITEMS: usize = 10_000;
/// Whether `value` (one page property) is cut at Notion's inline limit.
fn truncated(value: &Value) -> bool {
    match value["type"].as_str() {
        Some("relation") => value["has_more"] == true,
        Some("people") => value["people"].as_array().is_some_and(|a| a.len() >= 25),
        Some(kind @ ("title" | "rich_text")) => value[kind].as_array().is_some_and(|a| {
            a.iter()
                .filter(|item| {
                    item["type"] == "mention"
                        && matches!(item["mention"]["type"].as_str(), Some("page" | "user"))
                })
                .count()
                >= 25
        }),
        _ => false,
    }
}
/// Removes every property still cut at the inline limit, so the rest of the
/// page parses. Such a page must not be treated as complete.
pub(crate) fn drop_truncated(value: &mut Value) {
    if let Some(properties) = value["properties"].as_object_mut() {
        properties.retain(|_, property| {
            // The page needs its title, so the cut one stands in. Callers must
            // treat the page as incomplete, as the walk does.
            if property["type"] == "title" && truncated(property) {
                property[PAGINATED] = Value::Bool(true);
            }
            property[PAGINATED] == true || !truncated(property)
        });
    }
}
/// Percent-encodes an opaque cursor for a query string.
fn query_value(input: &str) -> String {
    input
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}
/// Notion property ids are already percent-encoded path segments.
fn segment(input: &str) -> bool {
    !input.is_empty()
        && input
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"%-._~".contains(&b))
}
fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.fetch_page",
        retry_after: None,
        committed_page_id: None,
    }
}
pub(crate) fn uuid(input: &str) -> Option<String> {
    if input.len() != 32
        && !(input.len() == 36 && [8, 13, 18, 23].iter().all(|i| input.as_bytes()[*i] == b'-'))
    {
        return None;
    }
    let raw = input.replace('-', "").to_ascii_lowercase();
    if raw.len() != 32 || !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!(
        "{}-{}-{}-{}-{}",
        &raw[..8],
        &raw[8..12],
        &raw[12..16],
        &raw[16..20],
        &raw[20..]
    ))
}
/// Accept UUIDs or HTTPS Notion page links; only the extracted ID is sent upstream.
pub fn page_id(input: &str) -> Result<PageId, BackendError> {
    let id = uuid(input)
        .or_else(|| {
            let url = Url::parse(input).ok()?;
            let host = url.host_str()?;
            if url.scheme() != "https"
                || !url.username().is_empty()
                || url.password().is_some()
                || url.port().is_some()
                || !(host == "app.notion.com"
                    || host == "notion.so"
                    || host.ends_with(".notion.so")
                    || host == "notion.site"
                    || host.ends_with(".notion.site"))
            {
                return None;
            }
            let segment = url.path_segments()?.next_back()?;
            uuid(segment).or_else(|| segment.get(segment.len().checked_sub(32)?..).and_then(uuid))
        })
        .ok_or_else(|| error(BackendErrorKind::InvalidInput))?;
    Ok(PageId(id))
}
/// Current responses use in_trash; page archival is a separate state.
/// Legacy archived remains accepted for recorded older-version fixtures.
pub(crate) fn inactive(value: &Value) -> Option<bool> {
    let mut found = false;
    let mut inactive = false;
    for field in ["in_trash", "is_archived", "archived"] {
        if let Some(value) = value.get(field) {
            inactive |= value.as_bool()?;
            found = true;
        }
    }
    found.then_some(inactive)
}
fn string(value: &Value) -> Result<String, BackendError> {
    value
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| error(BackendErrorKind::Internal))
}
fn rich_text(value: &Value) -> Result<String, BackendError> {
    value
        .as_array()
        .ok_or_else(|| error(BackendErrorKind::Internal))?
        .iter()
        .map(|v| string(&v["plain_text"]))
        .collect()
}
fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}
fn property(value: &Value) -> Result<PropertyValue, BackendError> {
    use PropertyValue::*;
    let kind = value["type"]
        .as_str()
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    if !matches!(
        kind,
        "title"
            | "rich_text"
            | "number"
            | "checkbox"
            | "url"
            | "email"
            | "phone_number"
            | "created_time"
            | "last_edited_time"
            | "select"
            | "status"
            | "multi_select"
            | "date"
            | "people"
            | "relation"
            | "created_by"
            | "last_edited_by"
            | "formula"
            | "rollup"
            | "string"
            | "boolean"
    ) {
        return Err(error(BackendErrorKind::UnsupportedContent));
    }
    let v = value
        .get(kind)
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    if v.is_null()
        && matches!(
            kind,
            "number" | "select" | "status" | "date" | "url" | "email" | "phone_number" | "string"
        )
    {
        return Ok(Null);
    }
    Ok(match kind {
        "title" | "rich_text" => {
            // Notion cuts text at 25 page or user mentions; only a text read
            // in full by `complete_properties` is whole.
            if value[PAGINATED] != true && truncated(value) {
                return Err(error(BackendErrorKind::UnsupportedContent));
            }
            Text(rich_text(v)?)
        }
        "number" => Number(
            v.as_f64()
                .ok_or_else(|| error(BackendErrorKind::Internal))?,
        ),
        "checkbox" => Boolean(
            v.as_bool()
                .ok_or_else(|| error(BackendErrorKind::Internal))?,
        ),
        "url" | "email" | "phone_number" | "created_time" | "last_edited_time" => Text(string(v)?),
        "select" | "status" => Text(string(&v["name"])?),
        "multi_select" => Strings(sorted(
            v.as_array()
                .ok_or_else(|| error(BackendErrorKind::Internal))?
                .iter()
                .map(|v| string(&v["name"]))
                .collect::<Result<_, _>>()?,
        )),
        "date" => Date {
            start: string(&v["start"])?,
            end: if v["end"].is_null() {
                None
            } else {
                Some(string(&v["end"])?)
            },
        },
        "people" | "relation" => {
            if kind == "relation" && !value["has_more"].is_boolean() {
                return Err(error(BackendErrorKind::Internal));
            }
            // Paginated by `complete_properties`: the list is whole.
            let complete = value[PAGINATED] == true;
            if !complete
                && (value["has_more"] == true
                    || (kind == "people" && v.as_array().is_some_and(|a| a.len() >= 25)))
            {
                return Err(error(BackendErrorKind::UnsupportedContent));
            }
            let ids = v
                .as_array()
                .ok_or_else(|| error(BackendErrorKind::Internal))?
                .iter()
                .map(|v| uuid(&string(&v["id"])?).ok_or_else(|| error(BackendErrorKind::Internal)))
                .collect::<Result<Vec<_>, _>>()?;
            if kind == "people" {
                PersonIds(sorted(ids))
            } else {
                PageIds(sorted(ids))
            }
        }
        "created_by" | "last_edited_by" => PersonIds(vec![
            uuid(&string(&v["id"])?).ok_or_else(|| error(BackendErrorKind::Internal))?,
        ]),
        "formula" | "rollup" => property(v)?,
        "string" => Text(string(v)?),
        "boolean" => Boolean(
            v.as_bool()
                .ok_or_else(|| error(BackendErrorKind::Internal))?,
        ),
        _ => return Err(error(BackendErrorKind::UnsupportedContent)),
    })
}
pub(crate) fn metadata(value: &Value, expected: &PageId) -> Result<Page, BackendError> {
    project_metadata(value, expected, false)
}

pub(crate) fn index_metadata(value: &Value, expected: &PageId) -> Result<Page, BackendError> {
    project_metadata(value, expected, true)
}

fn project_metadata(
    value: &Value,
    expected: &PageId,
    omit_array_rollups: bool,
) -> Result<Page, BackendError> {
    let id = uuid(&string(&value["id"])?).ok_or_else(|| error(BackendErrorKind::Internal))?;
    let url = string(&value["url"])?;
    let edited = string(&value["last_edited_time"])?;
    if value["object"] != "page"
        || id != expected.0
        || page_id(&url).map_err(|_| error(BackendErrorKind::Internal))? != *expected
        || chrono::DateTime::parse_from_rfc3339(&edited).is_err()
    {
        return Err(error(BackendErrorKind::Internal));
    }
    let archived = inactive(value).ok_or_else(|| error(BackendErrorKind::Internal))?;
    let mut properties = BTreeMap::new();
    let mut relation_names = BTreeMap::new();
    let mut title = None;
    let mut property_ids = BTreeSet::new();
    for (name, value) in value["properties"]
        .as_object()
        .ok_or_else(|| error(BackendErrorKind::Internal))?
        .iter()
    {
        let key = value["id"]
            .as_str()
            .ok_or_else(|| error(BackendErrorKind::Internal))?;
        if key.is_empty() || !property_ids.insert(key) {
            return Err(error(BackendErrorKind::Internal));
        }
        // Array rollups may contain heterogeneous derived values. They have no
        // faithful indexed filter representation, but the page body is intact.
        if omit_array_rollups
            && value["type"] == "rollup"
            && value["rollup"]["type"] == "array"
            && value["rollup"]["array"].is_array()
        {
            continue;
        }
        let normalized = property(value)?;
        if value["type"] == "relation" {
            relation_names.insert(key.to_owned(), name.clone());
        }
        if value["type"] == "title" {
            let PropertyValue::Text(text) = &normalized else {
                return Err(error(BackendErrorKind::Internal));
            };
            if title.replace(text.clone()).is_some() {
                return Err(error(BackendErrorKind::Internal));
            }
        }
        properties.insert(key.to_owned(), normalized);
    }
    Ok(Page {
        relation_names,
        id: PageId(id),
        url,
        title: title.ok_or_else(|| error(BackendErrorKind::Internal))?,
        last_edited_time: edited,
        archived,
        properties,
    })
}
impl NotionClient {
    /// Fresh exact metadata. Does not read page content, cache or follow links.
    pub async fn fetch_page(&self, input: &str) -> Result<Page, BackendError> {
        let id = page_id(input)?;
        let mut value = self.fetch_page_value(&id).await?;
        self.complete_properties(&id, &mut value, &mut 0).await?;
        metadata(&value, &id)
    }

    pub(crate) async fn fetch_source_page(&self, input: &str) -> Result<Page, BackendError> {
        let id = page_id(input)?;
        let mut value = self.fetch_page_value(&id).await?;
        if let Some(properties) = value["properties"].as_object_mut() {
            properties.retain(|_, property| property["type"] == "title");
        }
        self.complete_properties(&id, &mut value, &mut 0).await?;
        metadata(&value, &id)
    }

    /// Replaces each relation, people, title or rich-text property Notion cut
    /// at its inline 25-item limit with every item from the property item
    /// endpoint, so one
    /// long property costs that page extra reads instead of failing it.
    /// `calls` counts the reads made.
    pub(crate) async fn complete_properties(
        &self,
        id: &PageId,
        value: &mut Value,
        calls: &mut u64,
    ) -> Result<(), BackendError> {
        let Some(properties) = value["properties"].as_object() else {
            return Ok(());
        };
        let cut: Vec<(String, String, String)> = properties
            .iter()
            .filter(|(_, property)| truncated(property))
            .map(|(name, property)| {
                (
                    name.clone(),
                    property["id"].as_str().unwrap_or_default().to_owned(),
                    property["type"].as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect();
        for (name, key, kind) in cut {
            if !segment(&key) {
                return Err(error(BackendErrorKind::UnsupportedContent));
            }
            let mut items = Vec::new();
            let mut cursor: Option<String> = None;
            let mut cursors = std::collections::BTreeSet::new();
            loop {
                let mut path = format!("pages/{}/properties/{key}?page_size=100", id.0);
                if let Some(cursor) = &cursor {
                    path.push_str(&format!("&start_cursor={}", query_value(cursor)));
                }
                *calls += 1;
                // The page itself was readable: a 404/403 here is about this
                // property (for example a hidden related database), not the page.
                let list = self
                    .walk_request(reqwest::Method::GET, &path, None)
                    .await
                    .map_err(|e| match e.kind {
                        BackendErrorKind::NotFound | BackendErrorKind::PermissionDenied => {
                            error(BackendErrorKind::UnsupportedContent)
                        }
                        _ => e,
                    })?;
                let results = list["results"]
                    .as_array()
                    .filter(|_| list["object"] == "list")
                    .ok_or_else(|| error(BackendErrorKind::Internal))?;
                for item in results {
                    if item["object"] != "property_item" || item["type"] != kind.as_str() {
                        return Err(error(BackendErrorKind::Internal));
                    }
                    if matches!(kind.as_str(), "title" | "rich_text") {
                        // Each item is one whole rich-text object, in order.
                        let text = &item[kind.as_str()];
                        string(&text["plain_text"])?;
                        items.push(text.clone());
                    } else {
                        let id = string(&item[kind.as_str()]["id"])?;
                        items.push(serde_json::json!({ "id": id }));
                    }
                }
                if items.len() > MAX_PROPERTY_ITEMS {
                    return Err(error(BackendErrorKind::UnsupportedContent));
                }
                if list["has_more"] != true {
                    break;
                }
                // A missing, malformed or repeated cursor cannot prove the
                // list is whole.
                match list["next_cursor"].as_str() {
                    Some(next) if !next.is_empty() && cursors.insert(next.to_owned()) => {
                        cursor = Some(next.to_owned());
                    }
                    _ => return Err(error(BackendErrorKind::UnsupportedContent)),
                }
            }
            let property = &mut value["properties"][&name];
            property[kind.as_str()] = Value::Array(items);
            property["has_more"] = Value::Bool(false);
            property[PAGINATED] = Value::Bool(true);
        }
        Ok(())
    }

    /// The raw page object, for callers that also need its physical parent.
    pub(crate) async fn fetch_page_value(&self, id: &PageId) -> Result<Value, BackendError> {
        let request = self
            .http
            .get(format!("{}/pages/{}", self.api_root, id.0))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", PAGE_VERSION);
        let mut response = self.send(request, true, "notion.fetch_page").await?;
        if response.status().as_u16() != 200 {
            return Err(error(match response.status().as_u16() {
                400 => BackendErrorKind::InvalidInput,
                401 => BackendErrorKind::Unauthenticated,
                403 => BackendErrorKind::PermissionDenied,
                404 => BackendErrorKind::NotFound,
                409 => BackendErrorKind::Conflict,
                429 => BackendErrorKind::RateLimited,
                500..=599 => BackendErrorKind::Unavailable,
                _ => BackendErrorKind::Internal,
            }));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(BackendErrorKind::Unavailable))?
        {
            if body.len() + chunk.len() > 2 * 1024 * 1024 {
                return Err(error(BackendErrorKind::Internal));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| error(BackendErrorKind::Internal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const ID: &str = "12345678-1234-1234-1234-123456789abc";
    fn fixture() -> Value {
        json!({"object":"page","id":ID,"url":format!("https://www.notion.so/{ID}"),"archived":false,"last_edited_time":"2026-10-03T12:30:00.000Z","properties":{
            "Renamed title":{"id":"title","type":"title","title":[{"plain_text":"Hello"}]},
            "Count":{"id":"n%3A","type":"number","number":42},
            "Done":{"id":"c","type":"checkbox","checkbox":true},
            "Due":{"id":"d","type":"date","date":{"start":"2026-10-03","end":null}},
            "Related":{"id":"r","type":"relation","relation":[{"id":ID}],"has_more":false}
        }})
    }
    async fn fetch(input: &str, status: u16, body: String) -> (Result<Page, BackendError>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential")
            .unwrap()
            .with_fast_fixture_transport();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut bytes = [0; 2048];
                let n = stream.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&bytes[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            stream.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        (client.fetch_page(input).await, server.await.unwrap())
    }
    #[test]
    fn current_and_legacy_inactive_states_reject_malformed_flags() {
        for value in [
            json!({"in_trash":false,"is_archived":false}),
            json!({"archived":false}),
        ] {
            assert_eq!(inactive(&value), Some(false));
        }
        for value in [
            json!({"in_trash":true}),
            json!({"in_trash":false,"is_archived":true}),
            json!({"archived":true,"in_trash":false}),
        ] {
            assert_eq!(inactive(&value), Some(true));
        }
        for value in [
            json!({}),
            json!({"in_trash":"false"}),
            json!({"in_trash":false,"is_archived":null}),
        ] {
            assert_eq!(inactive(&value), None);
        }
    }

    #[tokio::test]
    async fn ids_and_normal_notion_links_fetch_identical_authoritative_metadata() {
        for input in [
            ID.to_owned(),
            ID.replace('-', "").to_uppercase(),
            format!(
                "https://www.notion.so/Workspace/Page-{}?pvs=4#block",
                ID.replace('-', "")
            ),
            format!("https://workspace.notion.site/{ID}"),
            format!("https://app.notion.com/p/Selected-{}", ID.replace('-', "")),
        ] {
            let (result, request) = fetch(&input, 200, fixture().to_string()).await;
            let page = result.unwrap();
            assert_eq!(page.id, PageId(ID.to_owned()));
            assert_eq!(page.title, "Hello");
            assert_eq!(page.last_edited_time, "2026-10-03T12:30:00.000Z");
            assert_eq!(page.properties["n%3A"], PropertyValue::Number(42.0));
            assert_eq!(
                page.properties["r"],
                PropertyValue::PageIds(vec![ID.to_owned()])
            );
            assert!(request.starts_with(&format!("GET /v1/pages/{ID} HTTP/1.1")));
            let headers = request.to_lowercase();
            assert!(headers.contains("authorization: bearer test-credential\r\n"));
            assert!(headers.contains(&format!("notion-version: {PAGE_VERSION}\r\n")));
        }
    }
    #[test]
    fn a_cut_title_stands_in_and_cut_text_is_dropped() {
        let mention =
            json!({"type":"mention","plain_text":"@x","mention":{"type":"user","user":{"id":ID}}});
        let mut value = fixture();
        value["properties"]["Renamed title"]["title"] = json!(vec![mention.clone(); 25]);
        value["properties"]["Text"] =
            json!({"id":"text","type":"rich_text","rich_text":vec![mention; 25]});
        let id = PageId(ID.to_owned());
        assert_eq!(
            metadata(&value, &id).unwrap_err().kind,
            BackendErrorKind::UnsupportedContent
        );
        drop_truncated(&mut value);
        let page = metadata(&value, &id).unwrap();
        assert_eq!(page.title, "@x".repeat(25));
        assert!(!page.properties.contains_key("text"));
    }
    #[tokio::test]
    async fn complete_properties_at_limits_preserve_values_and_types() {
        let mut value = fixture();
        value["properties"]["Related"]["relation"] = json!(vec![json!({"id":ID}); 25]);
        value["properties"]["Renamed title"]["title"] =
            json!(vec![json!({"type":"text","plain_text":"x"}); 30]);
        value["properties"]["Text"] =
            json!({"id":"text","type":"rich_text","rich_text":[{"plain_text":"Body"}]});
        value["properties"]["Tags"] =
            json!({"id":"tags","type":"multi_select","multi_select":[{"name":"A"},{"name":"B"}]});
        value["properties"]["Select"] = json!({"id":"select","type":"select","select":null});
        value["properties"]["People"] = json!({"id":"people","type":"people","people":[{"id":ID}]});
        value["properties"]["Formula"] =
            json!({"id":"formula","type":"formula","formula":{"type":"number","number":2}});
        value["properties"]["Rollup"] = json!({"id":"rollup","type":"rollup","rollup":{"type":"date","date":{"start":"2026-10-03","end":null}}});
        let page = fetch(ID, 200, value.to_string()).await.0.unwrap();
        assert_eq!(page.title, "x".repeat(30));
        assert_eq!(
            page.properties["r"],
            PropertyValue::PageIds(vec![ID.to_owned(); 25])
        );
        assert_eq!(
            page.properties["text"],
            PropertyValue::Text("Body".to_owned())
        );
        assert_eq!(
            page.properties["tags"],
            PropertyValue::Strings(vec!["A".to_owned(), "B".to_owned()])
        );
        assert_eq!(page.properties["select"], PropertyValue::Null);
        assert_eq!(
            page.properties["people"],
            PropertyValue::PersonIds(vec![ID.to_owned()])
        );
        assert_eq!(page.properties["formula"], PropertyValue::Number(2.0));
        assert_eq!(
            page.properties["rollup"],
            PropertyValue::Date {
                start: "2026-10-03".to_owned(),
                end: None
            }
        );
        // A cut list that cannot be paginated is never a whole page.
        value["properties"]["Related"]["has_more"] = json!(true);
        value["properties"]["Related"]["id"] = json!("not/a-segment");
        assert_eq!(
            fetch(ID, 200, value.to_string()).await.0.unwrap_err().kind,
            BackendErrorKind::UnsupportedContent
        );
    }
    #[tokio::test]
    async fn metadata_is_stable_across_display_names_and_collection_order() {
        let other = "aaaaaaaa-1234-1234-1234-123456789abc";
        let mut first = fixture();
        first["properties"]["Tags"] =
            json!({"id":"tags","type":"multi_select","multi_select":[{"name":"Z"},{"name":"A"}]});
        first["properties"]["People"] = json!({"id":"people","type":"people","people":[{"id":other.to_uppercase().replace('-', "")},{"id":ID}]});
        first["properties"]["Related"]["relation"] = json!([{"id":other},{"id":ID}]);
        first["properties"]["State"] =
            json!({"id":"state","type":"status","status":{"name":"In progress"}});
        first["properties"]["Choice"] =
            json!({"id":"choice","type":"select","select":{"name":"Blue"}});
        first["properties"]["Due"]["date"]["end"] = json!("2026-10-04");
        let mut second = first.clone();
        let properties = second["properties"].as_object_mut().unwrap();
        let renamed = properties.remove("Count").unwrap();
        properties.insert("A renamed numeric field".to_owned(), renamed);
        for (name, field) in [
            ("Tags", "multi_select"),
            ("People", "people"),
            ("Related", "relation"),
        ] {
            properties.get_mut(name).unwrap()[field]
                .as_array_mut()
                .unwrap()
                .reverse();
        }
        let page = fetch(ID, 200, first.to_string()).await.0.unwrap();
        let reordered = fetch(ID, 200, second.to_string()).await.0.unwrap();
        assert_eq!(page, reordered);
        assert_eq!(
            serde_json::to_string(&page.properties).unwrap(),
            serde_json::to_string(&reordered.properties).unwrap()
        );
        assert_eq!(
            page.properties["tags"],
            PropertyValue::Strings(vec!["A".into(), "Z".into()])
        );
        assert_eq!(
            page.properties["people"],
            PropertyValue::PersonIds(vec![ID.into(), other.into()])
        );
        assert_eq!(
            page.properties["r"],
            PropertyValue::PageIds(vec![ID.into(), other.into()])
        );
        assert_eq!(
            page.properties["state"],
            PropertyValue::Text("In progress".into())
        );
        assert_eq!(
            page.properties["choice"],
            PropertyValue::Text("Blue".into())
        );
        assert_eq!(page.properties["c"], PropertyValue::Boolean(true));
        assert_eq!(page.properties["n%3A"], PropertyValue::Number(42.0));
        assert_eq!(
            page.properties["d"],
            PropertyValue::Date {
                start: "2026-10-03".into(),
                end: Some("2026-10-04".into())
            }
        );
    }
    #[tokio::test]
    async fn unknown_property_types_fail_safely_without_exposing_values() {
        for payload in [
            json!({"id":"future","type":"future_type"}),
            json!({"id":"future","type":"future_type","future_type":"private property content"}),
        ] {
            let mut value = fixture();
            value["properties"]["Future"] = payload;
            let failure = fetch(ID, 200, value.to_string()).await.0.unwrap_err();
            assert_eq!(failure.kind, BackendErrorKind::UnsupportedContent);
            assert!(!format!("{failure:?} {failure}").contains("private property content"));
        }
        let mut value = fixture();
        value["properties"]["People"] =
            json!({"id":"people","type":"people","people":[{"id":"not-a-person-id"}]});
        assert_eq!(
            fetch(ID, 200, value.to_string()).await.0.unwrap_err().kind,
            BackendErrorKind::Internal
        );
    }
    #[tokio::test]
    async fn untrusted_links_and_invalid_ids_fail_before_network_access() {
        let client = NotionClient::integration("test-credential")
            .unwrap()
            .with_fast_fixture_transport();
        for input in [
            "garbage",
            "https://notion.so.evil.test/12345678123412341234123456789abc",
            "https://app.notion.com.evil.test/12345678123412341234123456789abc",
            "http://notion.so/12345678123412341234123456789abc",
            "https://user@notion.so/12345678123412341234123456789abc",
            "https://notion.so/",
            "12345678-1234-1234-1234-123456789abz",
        ] {
            assert_eq!(
                client.fetch_page(input).await.unwrap_err().kind,
                BackendErrorKind::InvalidInput
            );
        }
    }
    #[tokio::test]
    async fn missing_and_inaccessible_pages_have_sanitized_normalized_failures() {
        for (status, kind) in [
            (404, BackendErrorKind::NotFound),
            (403, BackendErrorKind::PermissionDenied),
            (401, BackendErrorKind::Unauthenticated),
            (429, BackendErrorKind::RateLimited),
            (503, BackendErrorKind::Unavailable),
            (302, BackendErrorKind::Internal),
        ] {
            let error = fetch(ID, status, "test-credential private content".to_owned())
                .await
                .0
                .unwrap_err();
            assert_eq!(error.kind, kind);
            assert_eq!(error.operation, "notion.fetch_page");
            assert!(!format!("{error:?} {error}").contains("test-credential"));
        }
    }
    #[tokio::test]
    async fn malformed_metadata_and_incomplete_properties_are_not_successful_pages() {
        let mut cases = vec![Value::Null];
        for key in ["id", "url", "last_edited_time", "archived", "properties"] {
            let mut value = fixture();
            value.as_object_mut().unwrap().remove(key);
            cases.push(value);
        }
        let mut value = fixture();
        value["last_edited_time"] = json!("yesterday");
        cases.push(value);
        let mut value = fixture();
        value["id"] = json!("aaaaaaaa-1234-1234-1234-123456789abc");
        cases.push(value);
        let mut value = fixture();
        value["properties"]["Count"]["number"] = json!("42");
        cases.push(value);
        for value in cases {
            assert_eq!(
                fetch(ID, 200, value.to_string()).await.0.unwrap_err().kind,
                BackendErrorKind::Internal
            );
        }
        let mut value = fixture();
        // A cut list that cannot be paginated is never a whole page.
        value["properties"]["Related"]["has_more"] = json!(true);
        value["properties"]["Related"]["id"] = json!("not/a-segment");
        assert_eq!(
            fetch(ID, 200, value.to_string()).await.0.unwrap_err().kind,
            BackendErrorKind::UnsupportedContent
        );
        assert_eq!(
            fetch(ID, 200, "x".repeat(2 * 1024 * 1024 + 1))
                .await
                .0
                .unwrap_err()
                .kind,
            BackendErrorKind::Internal
        );
    }
}
