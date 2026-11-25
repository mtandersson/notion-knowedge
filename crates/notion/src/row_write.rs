//! Authorized schema-aware Notion row values and durable create workflow.
use std::{collections::HashSet, future::Future, pin::Pin};

use chrono::{DateTime, NaiveDate, SecondsFormat, Utc};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageId},
    create::{CreateError as Error, valid_contents},
    discovery::SourceType,
    idempotency::{
        ExistingClaim, IdempotencyError, MutationClaim, MutationOperation, Reservation,
        VerifiedReceipt, WriteIdempotencyStore,
    },
    lifecycle::LifecycleScope,
    revision::{RevisionMetadata, written_markdown_matches},
    row_write::{
        CreateRowRequest, MAX_PROPERTIES, MAX_RELATIONS, PropertyPatch, PropertyValue, RowWrite,
        UpdateRowError, UpdateRowRequest,
    },
    write_refresh::{MutationRefreshQueue, RefreshKey, WriteCompletion, admit_verified},
};
use serde_json::{Map, Value, json};

use crate::{NotionClient, pages, views::row_revision, writes::encode_write_body};

const CREATE: &str = "notion.row.create";
const UPDATE: &str = "notion.row.update";

fn preflight(error: BackendError) -> Error {
    match error.kind {
        BackendErrorKind::NotFound
        | BackendErrorKind::PermissionDenied
        | BackendErrorKind::Unauthenticated => Error::NotAuthorized,
        BackendErrorKind::InvalidInput => Error::InvalidInput,
        _ => Error::Unavailable,
    }
}

fn ledger_error(error: IdempotencyError) -> Error {
    match error {
        IdempotencyError::InvalidInput => Error::InvalidInput,
        IdempotencyError::KeyConflict | IdempotencyError::AlreadyCommitted => {
            Error::IdempotencyConflict
        }
        _ => Error::Unavailable,
    }
}

fn safe_property_id(value: &str) -> bool {
    (1..=128).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_graphic())
}

fn canonical_uuid(value: &str) -> bool {
    pages::uuid(value).as_deref() == Some(value)
}

fn normalize_date(value: &str) -> Option<String> {
    if value.len() > 40 {
        return None;
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Some(date.format("%Y-%m-%d").to_string());
    }
    DateTime::parse_from_rfc3339(value).ok().map(|date| {
        date.with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::AutoSi, true)
    })
}

fn text_property(kind: &str, value: &str) -> Value {
    let content = if value.is_empty() {
        vec![]
    } else {
        vec![json!({"type":"text","text":{"content":value}})]
    };
    json!({(kind):content})
}

fn valid_http_url(value: &str) -> bool {
    value.len() <= 2000
        && reqwest::Url::parse(value).ok().is_some_and(|url| {
            matches!(url.scheme(), "https" | "http")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
        })
}

fn existing_option(schema: &Value, kind: &str, option: &str) -> bool {
    safe_property_id(option)
        && schema[kind]["options"]
            .as_array()
            .is_some_and(|items| items.iter().any(|entry| entry["id"] == option))
}

fn parent_is(value: &Value, kind: &str, expected: &str) -> bool {
    value["parent"]["type"] == kind
        && value["parent"][kind]
            .as_str()
            .and_then(pages::uuid)
            .as_deref()
            == Some(expected)
}

fn active_row(value: &Value, id: &str, data_source: &str) -> bool {
    value["object"] == "page"
        && value["id"].as_str().and_then(pages::uuid).as_deref() == Some(id)
        && pages::inactive(value) == Some(false)
        && parent_is(value, "data_source_id", data_source)
}

struct Prepared {
    properties: Map<String, Value>,
    relations: Vec<(String, String)>,
}

fn prepare_properties(schema: &Value, patch: &PropertyPatch) -> Result<Prepared, Error> {
    if patch.len() > MAX_PROPERTIES {
        return Err(Error::InvalidInput);
    }
    let schema = schema.as_object().ok_or(Error::Unavailable)?;
    let mut prepared = Prepared {
        properties: Map::new(),
        relations: Vec::new(),
    };
    for (id, property) in patch {
        if !safe_property_id(id) {
            return Err(Error::InvalidInput);
        }
        let mut definitions = schema.values().filter(|value| value["id"] == id.as_str());
        let definition = definitions
            .next()
            .filter(|_| definitions.next().is_none())
            .ok_or(Error::InvalidInput)?;
        let (kind, value) = match property {
            PropertyValue::Date(Some(date)) => {
                let start = normalize_date(&date.start).ok_or(Error::InvalidInput)?;
                let end = date
                    .end
                    .as_deref()
                    .map(|end| normalize_date(end).ok_or(Error::InvalidInput))
                    .transpose()?;
                if end
                    .as_ref()
                    .is_some_and(|end| (end.len() == 10) != (start.len() == 10))
                {
                    return Err(Error::InvalidInput);
                }
                ("date", json!({"start":start,"end":end}))
            }
            PropertyValue::Date(None) => ("date", Value::Null),
            PropertyValue::Status(option) if existing_option(definition, "status", option) => {
                ("status", json!({"id":option}))
            }
            PropertyValue::Select(Some(option))
                if existing_option(definition, "select", option) =>
            {
                ("select", json!({"id":option}))
            }
            PropertyValue::Select(None) => ("select", Value::Null),
            PropertyValue::Number(value) if value.is_none_or(f64::is_finite) => {
                ("number", json!(value))
            }
            PropertyValue::Checkbox(value) => ("checkbox", json!(value)),
            PropertyValue::Url(value) if value.as_deref().is_none_or(valid_http_url) => {
                ("url", json!(value))
            }
            PropertyValue::RichText(value)
                if value.chars().count() <= 2000
                    && value
                        .chars()
                        .all(|ch| !ch.is_control() || matches!(ch, '\n' | '\t')) =>
            {
                (
                    "rich_text",
                    text_property("rich_text", value)["rich_text"].clone(),
                )
            }
            PropertyValue::Relation(values)
                if values.len() <= MAX_RELATIONS
                    && values.iter().all(|id| canonical_uuid(id))
                    && values.iter().collect::<HashSet<_>>().len() == values.len() =>
            {
                let limit = &definition["relation"]["limit"];
                if !limit.is_null()
                    && (values.len() as u64 > limit.as_u64().ok_or(Error::Unavailable)?)
                {
                    return Err(Error::InvalidInput);
                }
                if !values.is_empty() {
                    let source = definition["relation"]["data_source_id"]
                        .as_str()
                        .and_then(pages::uuid)
                        .ok_or(Error::InvalidInput)?;
                    prepared
                        .relations
                        .extend(values.iter().map(|page| (source.clone(), page.clone())));
                }
                (
                    "relation",
                    json!(values.iter().map(|id| json!({"id":id})).collect::<Vec<_>>()),
                )
            }
            _ => return Err(Error::InvalidInput),
        };
        if definition["type"] != kind {
            return Err(Error::InvalidInput);
        }
        prepared
            .properties
            .insert(id.clone(), json!({(kind):value}));
    }
    if prepared.relations.len() > MAX_RELATIONS {
        return Err(Error::InvalidInput);
    }
    Ok(prepared)
}

fn matches_properties(value: &Value, expected: &Map<String, Value>) -> bool {
    let Some(properties) = value["properties"].as_object() else {
        return false;
    };
    expected.iter().all(|(id, property)| {
        let mut actual = properties
            .values()
            .filter(|value| value["id"] == id.as_str());
        let Some(actual_value) = actual.next().filter(|_| actual.next().is_none()) else {
            return false;
        };
        let Some((kind, expected_value)) = property.as_object().and_then(|p| p.iter().next())
        else {
            return false;
        };
        if actual_value["type"] != kind.as_str() || actual_value.get(kind).is_none() {
            return false;
        }
        let observed = &actual_value[kind];
        match kind.as_str() {
            "title" | "rich_text" => {
                let texts = |value: &Value, path: fn(&Value) -> Option<&str>| -> Option<String> {
                    let mut text = String::new();
                    for item in value.as_array()? {
                        text.push_str(path(item)?);
                    }
                    Some(text)
                };
                texts(observed, |item| {
                    (item["type"] == "text")
                        .then(|| item["plain_text"].as_str())
                        .flatten()
                })
                .zip(texts(expected_value, |item| {
                    item["text"]["content"].as_str()
                }))
                .is_some_and(|(observed, expected)| observed == expected)
            }
            "status" | "select" if !expected_value.is_null() => {
                observed["id"] == expected_value["id"]
            }
            "date" if !expected_value.is_null() => ["start", "end"].into_iter().all(|field| {
                if expected_value[field].is_null() {
                    observed[field].is_null()
                } else {
                    observed[field].as_str().and_then(normalize_date).as_deref()
                        == expected_value[field].as_str()
                }
            }),
            "relation" => {
                let ids = |value: &Value| {
                    value.as_array().and_then(|items| {
                        items
                            .iter()
                            .map(|item| item["id"].as_str().and_then(pages::uuid))
                            .collect::<Option<HashSet<_>>>()
                    })
                };
                actual_value["has_more"] != true
                    && ids(observed)
                        .zip(ids(expected_value))
                        .is_some_and(|(a, b)| a == b)
            }
            "number" if !expected_value.is_null() => observed.as_f64() == expected_value.as_f64(),
            _ => observed == expected_value,
        }
    })
}

impl NotionClient {
    async fn authorize_row_relations(
        &self,
        prepared: &Prepared,
        scope: &LifecycleScope,
    ) -> Result<(), Error> {
        let mut sources = HashSet::new();
        for (source, target) in &prepared.relations {
            if sources.insert(source) {
                self.scoped_data_source(source, scope)
                    .await
                    .map_err(preflight)?;
            }
            if scope
                .exclusions
                .exclusion(target, SourceType::Page)
                .is_some()
            {
                return Err(Error::NotAuthorized);
            }
            let page = self
                .fetch_page_value(&PageId(target.clone()))
                .await
                .map_err(preflight)?;
            if !active_row(&page, target, source) {
                return Err(Error::NotAuthorized);
            }
        }
        Ok(())
    }

    async fn verify_replayed_row(
        &self,
        receipt: &VerifiedReceipt,
        data_source: &str,
    ) -> Result<(), Error> {
        let existing = self
            .fetch_page_value(&PageId(receipt.page_id.clone()))
            .await
            .map_err(preflight)?;
        if !active_row(&existing, &receipt.page_id, data_source) {
            return Err(Error::NotAuthorized);
        }
        Ok(())
    }

    async fn verify_created_row(
        &self,
        receipt: &Value,
        request: &CreateRowRequest,
        prepared: &Prepared,
        scope: &LifecycleScope,
    ) -> Result<VerifiedReceipt, Error> {
        let id = receipt["id"]
            .as_str()
            .and_then(pages::uuid)
            .ok_or(Error::OutcomeUnknown)?;
        if !active_row(receipt, &id, &request.data_source_id) {
            return Err(Error::OutcomeUnknown);
        }
        let id = PageId(id);
        let first = self
            .fetch_page_value(&id)
            .await
            .map_err(|_| Error::OutcomeUnknown)?;
        let markdown = self
            .read_markdown(&id)
            .await
            .map_err(|_| Error::OutcomeUnknown)?;
        let last = self
            .fetch_page_value(&id)
            .await
            .map_err(|_| Error::OutcomeUnknown)?;
        if !active_row(&first, &id.0, &request.data_source_id)
            || !active_row(&last, &id.0, &request.data_source_id)
            || first["last_edited_time"] != last["last_edited_time"]
            || first["url"] != last["url"]
            || receipt["url"] != last["url"]
            || !matches_properties(&first, &prepared.properties)
            || !matches_properties(&last, &prepared.properties)
            || !written_markdown_matches(&request.markdown, &markdown)
        {
            return Err(Error::OutcomeUnknown);
        }
        self.scoped_data_source(&request.data_source_id, scope)
            .await
            .map_err(|_| Error::OutcomeUnknown)?;
        VerifiedReceipt::from_readback(
            id.0,
            last["url"].as_str().ok_or(Error::OutcomeUnknown)?.into(),
            last["last_edited_time"]
                .as_str()
                .ok_or(Error::OutcomeUnknown)?
                .into(),
        )
        .map_err(|_| Error::OutcomeUnknown)
    }
}

fn check_row_revision(row: &Value, id: &PageId, expected: &str) -> Result<(), UpdateRowError> {
    let current = row_revision(row).ok_or(Error::Unavailable)?;
    if expected != current {
        return Err(UpdateRowError::RevisionConflict(RevisionMetadata {
            page_id: id.clone(),
            last_edited_time: row["last_edited_time"]
                .as_str()
                .ok_or(Error::Unavailable)?
                .into(),
        }));
    }
    Ok(())
}

impl RowWrite for NotionClient {
    fn create_row<'a>(
        &'a self,
        request: CreateRowRequest,
        scope: &'a LifecycleScope,
        ledger: &'a dyn WriteIdempotencyStore,
        queue: &'a dyn MutationRefreshQueue,
    ) -> Pin<Box<dyn Future<Output = Result<WriteCompletion, Error>> + Send + 'a>> {
        Box::pin(async move {
            if !canonical_uuid(&request.data_source_id)
                || !valid_contents(&request.title, &request.markdown)
                || request.properties.len() > MAX_PROPERTIES
            {
                return Err(Error::InvalidInput);
            }
            let payload = json!([request.title, request.markdown, request.properties]);
            let claim = MutationClaim::new(
                &scope.workspace_id,
                &request.idempotency_key,
                MutationOperation::CreateRow,
                &request.data_source_id,
                &MutationClaim::payload_sha256(payload.to_string().as_bytes()),
            )
            .map_err(ledger_error)?;
            let (schema, _) = self
                .scoped_data_source(&request.data_source_id, scope)
                .await
                .map_err(preflight)?;
            let (receipt, replayed) = match ledger.lookup(&claim).map_err(ledger_error)? {
                Some(ExistingClaim::Replay(receipt)) => (receipt, true),
                Some(ExistingClaim::Reconcile) => return Err(Error::OutcomeUnknown),
                None => {
                    let schema = &schema["properties"];
                    let mut prepared = prepare_properties(schema, &request.properties)?;
                    let mut titles = schema
                        .as_object()
                        .ok_or(Error::Unavailable)?
                        .values()
                        .filter(|p| p["type"] == "title");
                    let title_id = titles
                        .next()
                        .filter(|_| titles.next().is_none())
                        .and_then(|p| p["id"].as_str())
                        .filter(|id| safe_property_id(id))
                        .ok_or(Error::Unavailable)?;
                    prepared
                        .properties
                        .insert(title_id.into(), text_property("title", &request.title));
                    if !prepared.relations.is_empty() {
                        self.authorize_row_relations(&prepared, scope).await?;
                        let (current, _) = self
                            .scoped_data_source(&request.data_source_id, scope)
                            .await
                            .map_err(preflight)?;
                        if current["properties"] != *schema {
                            return Err(Error::Unavailable);
                        }
                    }
                    let body = json!({
                        "parent":{"type":"data_source_id","data_source_id":request.data_source_id},
                        "properties":prepared.properties,"markdown":request.markdown
                    });
                    encode_write_body(&body, CREATE).map_err(preflight)?;
                    match ledger.begin(&claim).map_err(ledger_error)? {
                        Reservation::Replay(receipt) => (receipt, true),
                        Reservation::Reconcile => return Err(Error::OutcomeUnknown),
                        Reservation::ExecuteOnce => {
                            ledger.mark_uncertain(&claim).map_err(ledger_error)?;
                            let created = self
                                .write(reqwest::Method::POST, "/pages", body, CREATE)
                                .await
                                .map_err(|_| Error::OutcomeUnknown)?;
                            let receipt = self
                                .verify_created_row(&created, &request, &prepared, scope)
                                .await?;
                            ledger
                                .record_verified(&claim, &receipt)
                                .map_err(|_| Error::OutcomeUnknown)?;
                            (receipt, false)
                        }
                    }
                }
            };
            if replayed {
                self.verify_replayed_row(&receipt, &request.data_source_id)
                    .await?;
            }
            Ok(admit_verified(
                queue,
                &RefreshKey {
                    workspace_id: scope.workspace_id.clone(),
                    mutation_digest: claim.key_digest().into(),
                    page_id: receipt.page_id.clone(),
                },
                receipt,
                replayed,
            ))
        })
    }
    fn update_row<'a>(
        &'a self,
        request: UpdateRowRequest,
        scope: &'a LifecycleScope,
        ledger: &'a dyn WriteIdempotencyStore,
        queue: &'a dyn MutationRefreshQueue,
    ) -> Pin<Box<dyn Future<Output = Result<WriteCompletion, UpdateRowError>> + Send + 'a>> {
        Box::pin(async move {
            if !canonical_uuid(&request.page_id)
                || request.expected_row_revision.len() != 64
                || !request
                    .expected_row_revision
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || !(1..=MAX_PROPERTIES).contains(&request.properties.len())
                || request
                    .properties
                    .values()
                    .any(|value| matches!(value, PropertyValue::Relation(_)))
            {
                return Err(Error::InvalidInput.into());
            }

            let id = PageId(request.page_id);
            if scope
                .exclusions
                .exclusion(&id.0, SourceType::Page)
                .is_some()
            {
                return Err(Error::NotAuthorized.into());
            }
            let payload = json!([request.expected_row_revision, request.properties]);
            let claim = MutationClaim::new(
                &scope.workspace_id,
                &request.idempotency_key,
                MutationOperation::UpdateRow,
                &id.0,
                &MutationClaim::payload_sha256(payload.to_string().as_bytes()),
            )
            .map_err(ledger_error)?;
            let original = self.fetch_page_value(&id).await.map_err(preflight)?;
            let data_source = original["parent"]["data_source_id"]
                .as_str()
                .and_then(pages::uuid)
                .filter(|source| active_row(&original, &id.0, source))
                .ok_or(Error::NotAuthorized)?;
            let (schema, _) = self
                .scoped_data_source(&data_source, scope)
                .await
                .map_err(preflight)?;

            let (receipt, replayed) = match ledger.lookup(&claim).map_err(ledger_error)? {
                Some(ExistingClaim::Replay(receipt)) if receipt.page_id == id.0 => (receipt, true),
                Some(ExistingClaim::Replay(_)) | Some(ExistingClaim::Reconcile) => {
                    return Err(Error::OutcomeUnknown.into());
                }
                None => {
                    check_row_revision(&original, &id, &request.expected_row_revision)?;
                    let prepared = prepare_properties(&schema["properties"], &request.properties)?;
                    let body = json!({"properties":prepared.properties});
                    encode_write_body(&body, UPDATE).map_err(preflight)?;

                    // A second fresh read after checking the schema and physical
                    // source catches intervening edits before opening the claim.
                    // Notion PATCH still has no atomic conditional update.
                    let current = self.fetch_page_value(&id).await.map_err(preflight)?;
                    if !active_row(&current, &id.0, &data_source) {
                        return Err(Error::NotAuthorized.into());
                    }
                    check_row_revision(&current, &id, &request.expected_row_revision)?;
                    match ledger.begin(&claim).map_err(ledger_error)? {
                        Reservation::Replay(receipt) if receipt.page_id == id.0 => (receipt, true),
                        Reservation::Replay(_) | Reservation::Reconcile => {
                            return Err(Error::OutcomeUnknown.into());
                        }
                        Reservation::ExecuteOnce => {
                            ledger.mark_uncertain(&claim).map_err(ledger_error)?;
                            let accepted = self
                                .write(
                                    reqwest::Method::PATCH,
                                    &format!("/pages/{}", id.0),
                                    body,
                                    UPDATE,
                                )
                                .await
                                .map_err(|_| Error::OutcomeUnknown)?;
                            if !active_row(&accepted, &id.0, &data_source) {
                                return Err(Error::OutcomeUnknown.into());
                            }
                            let readback = self
                                .fetch_page_value(&id)
                                .await
                                .map_err(|_| Error::OutcomeUnknown)?;
                            if !active_row(&readback, &id.0, &data_source)
                                || readback["url"] != accepted["url"]
                                || !matches_properties(&readback, &prepared.properties)
                            {
                                return Err(Error::OutcomeUnknown.into());
                            }
                            self.scoped_data_source(&data_source, scope)
                                .await
                                .map_err(|_| Error::OutcomeUnknown)?;
                            let receipt = VerifiedReceipt::from_readback(
                                id.0.clone(),
                                readback["url"]
                                    .as_str()
                                    .ok_or(Error::OutcomeUnknown)?
                                    .into(),
                                readback["last_edited_time"]
                                    .as_str()
                                    .ok_or(Error::OutcomeUnknown)?
                                    .into(),
                            )
                            .map_err(|_| Error::OutcomeUnknown)?;
                            ledger
                                .record_verified(&claim, &receipt)
                                .map_err(|_| Error::OutcomeUnknown)?;
                            (receipt, false)
                        }
                    }
                }
            };
            Ok(admit_verified(
                queue,
                &RefreshKey {
                    workspace_id: scope.workspace_id.clone(),
                    mutation_digest: claim.key_digest().into(),
                    page_id: id.0,
                },
                receipt,
                replayed,
            ))
        })
    }
}
