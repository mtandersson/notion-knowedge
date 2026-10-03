//! Exact authoritative page metadata reads. No content or credential logging.
use crate::NotionClient;
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, Page, PageId},
    indexed::PropertyValue,
};
use reqwest::{Url, header::AUTHORIZATION};
use serde_json::Value;
use std::collections::BTreeMap;

pub const PAGE_VERSION: &str = "2026-03-11";
fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.fetch_page",
        retry_after: None,
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
                || !(host == "notion.so"
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
            if v.as_array().is_some_and(|a| {
                a.iter()
                    .filter(|item| {
                        item["type"] == "mention"
                            && matches!(item["mention"]["type"].as_str(), Some("page" | "user"))
                    })
                    .count()
                    >= 25
            }) {
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
            if value["has_more"] == true
                || (kind == "people" && v.as_array().is_some_and(|a| a.len() >= 25))
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
fn metadata(value: &Value, expected: &PageId) -> Result<Page, BackendError> {
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
    let archived = value["archived"]
        .as_bool()
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    let mut properties = BTreeMap::new();
    let mut title = None;
    for value in value["properties"]
        .as_object()
        .ok_or_else(|| error(BackendErrorKind::Internal))?
        .values()
    {
        let key = string(&value["id"])?;
        if key.is_empty() {
            return Err(error(BackendErrorKind::Internal));
        }
        let normalized = property(value)?;
        if value["type"] == "title" {
            let PropertyValue::Text(text) = &normalized else {
                return Err(error(BackendErrorKind::Internal));
            };
            if title.replace(text.clone()).is_some() {
                return Err(error(BackendErrorKind::Internal));
            }
        }
        if properties.insert(key, normalized).is_some() {
            return Err(error(BackendErrorKind::Internal));
        }
    }
    Ok(Page {
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
        let value = serde_json::from_slice(&body).map_err(|_| error(BackendErrorKind::Internal))?;
        metadata(&value, &id)
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
            .without_retries();
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
        value["properties"]["Related"]["has_more"] = json!(true);
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
            .without_retries();
        for input in [
            "garbage",
            "https://notion.so.evil.test/12345678123412341234123456789abc",
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
        value["properties"]["Related"]["has_more"] = json!(true);
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
