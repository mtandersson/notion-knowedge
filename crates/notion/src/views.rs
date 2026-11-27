//! Authoritative saved-view execution and physical container authorization.
use std::collections::HashSet;

use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind as Kind, PageId, ViewQueryError},
    discovery::SourceType,
    lifecycle::LifecycleScope,
};
use reqwest::Method;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::{NotionClient, pages};

/// Views fetched per listing; a database with more reports `truncated`.
const MAX_LISTED_VIEWS: usize = 25;
const MAX_VIEW_ROW_PAYLOAD_BYTES: usize = 512 * 1024;

fn failure(kind: Kind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.view_query",
        retry_after: None,
        committed_page_id: None,
    }
}

/// A scope denial that names the stage in the log without changing the
/// generic text returned to the caller.
fn denied(stage: &'static str) -> BackendError {
    BackendError {
        kind: Kind::PermissionDenied,
        operation: stage,
        retry_after: None,
        committed_page_id: None,
    }
}

fn id(value: &Value) -> Result<String, BackendError> {
    value
        .as_str()
        .and_then(pages::uuid)
        .ok_or_else(|| failure(Kind::Internal))
}

fn object(value: &Value, kind: &str, expected: &str) -> Result<(), BackendError> {
    if value["object"] != kind || id(&value["id"])? != expected {
        return Err(failure(Kind::Internal));
    }
    Ok(())
}

pub fn normalize_view_id(reference: &str) -> Option<String> {
    pages::uuid(reference.strip_prefix("view://").unwrap_or(reference))
}

fn plain_text(value: &Value) -> Option<Value> {
    let elements = value.as_array()?;
    let mut text = String::new();
    for element in elements {
        if element["type"] != "text" {
            return None;
        }
        text.push_str(element["plain_text"].as_str()?);
    }
    Some(json!(text))
}

fn option(value: &Value) -> Option<Value> {
    Some(json!({"id":value["id"].as_str()?, "name":value["name"].as_str()?}))
}

/// Only intrinsic values of the authorized row. Notion's inline relation,
/// rollup and media payloads can refer to resources outside this scope.
fn project_value(property: &Value) -> Option<Value> {
    let kind = property["type"].as_str()?;
    let value = property.get(kind)?;
    Some(match kind {
        "title" | "rich_text" => plain_text(value)?,
        "select" | "status" => {
            if value.is_null() {
                Value::Null
            } else {
                option(value)?
            }
        }
        "multi_select" => Value::Array(
            value
                .as_array()?
                .iter()
                .map(option)
                .collect::<Option<_>>()?,
        ),
        "date" if value.is_object() => {
            let mut date = Map::new();
            date.insert("start".into(), json!(value["start"].as_str()?));
            for key in ["end", "time_zone"] {
                if let Some(text) = value[key].as_str() {
                    date.insert(key.into(), json!(text));
                }
            }
            Value::Object(date)
        }
        "date" if value.is_null() => Value::Null,
        "number" if value.is_number() || value.is_null() => value.clone(),
        "checkbox" if value.is_boolean() => value.clone(),
        // This is the row's user-entered URL, not a generated file URL.
        "url" | "email" | "phone_number" if value.is_string() || value.is_null() => value.clone(),
        "created_time" | "last_edited_time" if value.is_string() => value.clone(),
        _ => return None,
    })
}

const COLUMN_TYPES: [&str; 13] = [
    "title",
    "rich_text",
    "select",
    "status",
    "multi_select",
    "date",
    "number",
    "checkbox",
    "url",
    "email",
    "phone_number",
    "created_time",
    "last_edited_time",
];

/// Which columns and optional per-row fields a view response carries.
#[derive(Clone, Copy, Default)]
pub struct ViewProjection<'a> {
    pub fields: Option<&'a [String]>,
    pub row_revision: bool,
}

struct Column {
    id: String,
    name: String,
    kind: String,
}

/// Columns come from the data-source schema, so they are the same for every
/// row and for an empty result. The title column is always first.
fn columns(
    schema: &Value,
    fields: Option<&[String]>,
) -> Result<(Vec<Column>, Vec<Value>), ViewQueryError> {
    let properties = schema["properties"]
        .as_object()
        .ok_or_else(|| failure(Kind::Internal))?;
    let mut all = Vec::with_capacity(properties.len());
    for (name, property) in properties {
        all.push(Column {
            id: property["id"]
                .as_str()
                .ok_or_else(|| failure(Kind::Internal))?
                .to_owned(),
            name: name.clone(),
            kind: property["type"]
                .as_str()
                .ok_or_else(|| failure(Kind::Internal))?
                .to_owned(),
        });
    }
    let mut titles = all.iter().filter(|column| column.kind == "title");
    let title = titles.next().ok_or_else(|| failure(Kind::Internal))?;
    if titles.next().is_some() {
        return Err(failure(Kind::Internal).into());
    }
    let title_id = title.id.clone();
    let mut selected: Vec<usize> = Vec::new();
    let mut omitted = Vec::new();
    match fields {
        None => selected.extend(0..all.len()),
        Some(fields) => {
            for field in fields {
                let by_id = all.iter().position(|column| &column.id == field);
                let by_name = all.iter().position(|column| &column.name == field);
                if by_id.is_some() && by_name.is_some() && by_id != by_name {
                    return Err(failure(Kind::InvalidInput).into());
                }
                let Some(index) = by_id.or(by_name) else {
                    return Err(ViewQueryError::UnknownField {
                        columns: all.iter().map(|column| column.name.clone()).collect(),
                    });
                };
                if !selected.contains(&index) {
                    selected.push(index);
                }
            }
        }
    }
    let mut kept = Vec::new();
    for index in selected {
        let column = &all[index];
        if COLUMN_TYPES.contains(&column.kind.as_str()) {
            kept.push(index);
        } else {
            omitted.push(json!({"id":column.id, "name":column.name, "type":column.kind}));
        }
    }
    kept.sort_by_key(|&index| all[index].id != title_id);
    if kept.first().is_none_or(|&index| all[index].id != title_id) {
        kept.insert(0, all.iter().position(|c| c.id == title_id).unwrap());
    }
    let mut taken: Vec<Option<Column>> = all.into_iter().map(Some).collect();
    Ok((
        kept.into_iter()
            .filter_map(|index| taken[index].take())
            .collect(),
        omitted,
    ))
}

/// Fingerprint the source revision and stable property values. Notion can give
/// different values the same timestamp. Generated file URLs and derived
/// relation/rollup properties vary independently of a patchable row.
pub(crate) fn row_revision(row: &Value) -> Option<String> {
    let properties = row["properties"].as_object()?;
    let selected = properties.iter().filter(|(_, property)| {
        matches!(
            property["type"].as_str(),
            Some(
                "title"
                    | "rich_text"
                    | "status"
                    | "select"
                    | "multi_select"
                    | "date"
                    | "url"
                    | "number"
                    | "checkbox"
            )
        )
    });
    if selected
        .clone()
        .any(|(_, property)| pages::truncated(property))
    {
        return None;
    }
    let fields: Map<String, Value> = selected
        .map(|(name, field)| (name.clone(), field.clone()))
        .collect();
    let time = row["last_edited_time"].as_str()?;
    chrono::DateTime::parse_from_rfc3339(time).ok()?;
    let snapshot = json!([row["id"], row["parent"], time, fields]);
    Some(format!(
        "{:x}",
        Sha256::digest(snapshot.to_string().as_bytes())
    ))
}

struct View {
    value: Value,
    schema: Value,
    database: String,
    source: String,
    source_database: String,
}

impl View {
    fn fingerprint(&self) -> Result<String, BackendError> {
        let value = &self.value;
        if !value["last_edited_time"].is_string() {
            return Err(failure(Kind::Internal));
        }
        let fields = json!([
            value["id"],
            self.database,
            self.source,
            self.source_database,
            value["last_edited_time"],
            value["filter"],
            value["sorts"],
            value["quick_filters"]
        ]);
        Ok(format!(
            "{:x}",
            Sha256::digest(fields.to_string().as_bytes())
        ))
    }
}

impl NotionClient {
    async fn view_object(&self, kind: &str, id: &str) -> Result<Value, BackendError> {
        let stage = match kind {
            "views" => "fetch.view",
            "data_sources" => "fetch.data_source",
            "databases" => "fetch.database",
            "pages" => "fetch.page",
            _ => "fetch.block",
        };
        self.walk_request(Method::GET, &format!("{kind}/{id}"), None)
            .await
            .map_err(|error| BackendError {
                operation: stage,
                ..error
            })
    }

    /// Follow current physical parents. A link to a resource never grants access to it.
    async fn verify_database_scope(
        &self,
        database: &str,
        linked: bool,
        scope: &LifecycleScope,
    ) -> Result<(), BackendError> {
        let mut kind = "database_id";
        let mut current = database.to_owned();
        let mut visited = HashSet::new();
        for _ in 0..32 {
            let (endpoint, object_kind, source_type) = match kind {
                "database_id" => ("databases", "database", Some(SourceType::Database)),
                "data_source_id" => ("data_sources", "data_source", Some(SourceType::DataSource)),
                "page_id" => ("pages", "page", Some(SourceType::Page)),
                "block_id" => ("blocks", "block", None),
                _ => return Err(denied("scope.unsupported_container")),
            };
            if !visited.insert((kind, current.clone())) {
                return Err(denied("scope.cycle"));
            }
            if source_type
                .is_some_and(|source| scope.exclusions.exclusion(&current, source).is_some())
                || (kind == "page_id" && scope.exclusions.descendants_of.contains(&current))
            {
                return Err(denied("scope.excluded"));
            }
            let value = match self.view_object(endpoint, &current).await {
                // Notion's database endpoint can omit linked containers. The
                // exact child-database block can still prove physical placement.
                Err(error)
                    if linked
                        && kind == "database_id"
                        && current == database
                        && error.kind == Kind::NotFound =>
                {
                    let block = self.view_object("blocks", &current).await?;
                    object(&block, "block", &current)?;
                    if block["type"] != "child_database" {
                        return Err(denied("scope.linked_not_child_database"));
                    }
                    block
                }
                result => {
                    let value = result?;
                    object(&value, object_kind, &current)?;
                    value
                }
            };
            // Data source objects have no archive flag. Their database must be active.
            match (kind, pages::inactive(&value)) {
                (_, Some(true)) => return Err(denied("scope.inactive")),
                ("database_id" | "page_id" | "block_id", None) => {
                    return Err(denied("scope.unknown_active_state"));
                }
                _ => {}
            }
            if kind == "page_id" && scope.roots.iter().any(|root| root.0 == current) {
                return Ok(());
            }
            let parent = &value["parent"];
            kind = match parent["type"].as_str() {
                Some("database_id") => "database_id",
                Some("data_source_id") => "data_source_id",
                Some("page_id") => "page_id",
                Some("block_id") => "block_id",
                _ => return Err(denied("scope.unsupported_parent")),
            };
            current = id(&parent[kind])?;
        }
        Err(denied("scope.depth_exceeded"))
    }

    /// Resolve the actual owner of a data source with live root authorization.
    pub(crate) async fn scoped_data_source(
        &self,
        source: &str,
        scope: &LifecycleScope,
    ) -> Result<(Value, String), BackendError> {
        if scope
            .exclusions
            .exclusion(source, SourceType::DataSource)
            .is_some()
        {
            return Err(denied("data_source.excluded"));
        }
        let metadata = self.view_object("data_sources", source).await?;
        object(&metadata, "data_source", source)?;
        if metadata["parent"]["type"] != "database_id" {
            return Err(denied("data_source.parent_not_database"));
        }
        if pages::inactive(&metadata) == Some(true) {
            return Err(denied("data_source.inactive"));
        }
        let source_database = id(&metadata["parent"]["database_id"])?;
        self.verify_database_scope(&source_database, false, scope)
            .await?;
        Ok((metadata, source_database))
    }

    async fn verified_view(
        &self,
        view: &str,
        scope: &LifecycleScope,
    ) -> Result<View, BackendError> {
        let value = self.view_object("views", view).await?;
        object(&value, "view", view)?;
        if value["parent"]["type"] != "database_id"
            || !matches!(
                value["type"].as_str(),
                Some("table" | "list" | "board" | "calendar" | "timeline" | "gallery")
            )
            || value["data_source_id"].is_null()
        {
            return Err(failure(Kind::UnsupportedContent));
        }
        let database = id(&value["parent"]["database_id"])?;
        let source = id(&value["data_source_id"])?;
        let (schema, source_database) = self.scoped_data_source(&source, scope).await?;
        if source_database != database {
            self.verify_database_scope(&database, true, scope).await?;
        }
        Ok(View {
            value,
            schema,
            database,
            source,
            source_database,
        })
    }

    /// List the saved views of one data source: the views that live on the
    /// data source's own database and read from it. Notion's list endpoint
    /// returns bare ids, so each view is fetched for its name and type. Views
    /// elsewhere (linked views) are not listed, which keeps every listed view
    /// inside the scope already proven for the owning database.
    pub async fn list_saved_views(
        &self,
        scope: &LifecycleScope,
        reference: &str,
    ) -> Result<Value, BackendError> {
        let source = pages::uuid(reference).ok_or_else(|| failure(Kind::InvalidInput))?;
        let (_, database) = self.scoped_data_source(&source, scope).await?;
        let listing = self
            .walk_request(
                Method::GET,
                &format!("views?database_id={database}&page_size={MAX_LISTED_VIEWS}"),
                None,
            )
            .await?;
        let ids = match (listing["object"].as_str(), listing["results"].as_array()) {
            (Some("list"), Some(results)) => results,
            _ => return Err(failure(Kind::Internal)),
        };
        let mut views = Vec::new();
        for entry in ids {
            let view_id = id(&entry["id"])?;
            let view = self.view_object("views", &view_id).await?;
            object(&view, "view", &view_id)?;
            let queryable = matches!(
                view["type"].as_str(),
                Some("table" | "list" | "board" | "calendar" | "timeline" | "gallery")
            );
            if !queryable
                || view["parent"]["database_id"]
                    .as_str()
                    .and_then(pages::uuid)
                    .as_deref()
                    != Some(database.as_str())
                || view["data_source_id"]
                    .as_str()
                    .and_then(pages::uuid)
                    .as_deref()
                    != Some(source.as_str())
            {
                continue;
            }
            views.push(json!({
                "id": view_id,
                "name": view["name"].as_str().unwrap_or(""),
                "type": view["type"],
            }));
        }
        Ok(json!({
            "data_source_id": source,
            "database_id": database,
            "status": if views.is_empty() { "empty" } else { "ok" },
            "views": views,
            "truncated": listing["has_more"] == true,
        }))
    }

    pub async fn query_saved_view(
        &self,
        scope: &LifecycleScope,
        reference: &str,
        page_size: usize,
        continuation: Option<(&str, &str, &str)>,
        projection: ViewProjection<'_>,
    ) -> Result<Value, ViewQueryError> {
        if !(1..=25).contains(&page_size) {
            return Err(failure(Kind::InvalidInput).into());
        }
        let view_id = normalize_view_id(reference).ok_or_else(|| failure(Kind::InvalidInput))?;
        let view = self.verified_view(&view_id, scope).await?;
        let revision = view.fingerprint()?;
        let (columns, omitted_columns) = columns(&view.schema, projection.fields)?;
        let path = format!("views/{view_id}/queries");
        let (response, query_id) = match continuation {
            Some((query, cursor, expected_revision)) => {
                if revision != expected_revision {
                    return Err(failure(Kind::Conflict).into());
                }
                if cursor.is_empty() || cursor.len() > 256 || cursor.chars().any(char::is_control) {
                    return Err(failure(Kind::InvalidInput).into());
                }
                let query = pages::uuid(query).ok_or_else(|| failure(Kind::InvalidInput))?;
                let cursor = pages::query_value(cursor);
                let response = self
                    .walk_request(
                        Method::GET,
                        &format!("{path}/{query}?page_size={page_size}&start_cursor={cursor}"),
                        None,
                    )
                    .await
                    .map_err(|error| {
                        if error.kind == Kind::NotFound {
                            failure(Kind::Conflict)
                        } else {
                            error
                        }
                    })?;
                if response["object"] != "list" || response["type"] != "page" {
                    return Err(failure(Kind::Internal).into());
                }
                (response, query)
            }
            None => {
                let response = self
                    .walk_request(Method::POST, &path, Some(&json!({"page_size":page_size})))
                    .await?;
                if response["object"] != "view_query" || id(&response["view_id"])? != view_id {
                    return Err(failure(Kind::Internal).into());
                }
                let query = id(&response["id"])?;
                (response, query)
            }
        };
        let entries = response["results"]
            .as_array()
            .filter(|rows| rows.len() <= page_size)
            .ok_or_else(|| failure(Kind::Internal))?;
        let next = match (
            response["has_more"].as_bool(),
            response["next_cursor"].as_str(),
        ) {
            (Some(true), Some(cursor))
                if !cursor.is_empty()
                    && cursor.len() <= 256
                    && !cursor.chars().any(char::is_control)
                    && continuation.is_none_or(|(_, previous, _)| previous != cursor) =>
            {
                json!({"query_id":query_id, "cursor":cursor, "view_revision":revision})
            }
            (Some(false), None) if response["next_cursor"].is_null() => Value::Null,
            _ => return Err(failure(Kind::Internal).into()),
        };
        let truncated = match response["request_status"]["type"].as_str() {
            Some("incomplete") => true,
            Some("complete") => false,
            None if response["request_status"].is_null() => false,
            _ => return Err(failure(Kind::Internal).into()),
        };
        // Query results carry references. Resolve each row in result order and
        // revalidate its active physical source before exposing its properties.
        let mut ids = HashSet::new();
        let mut rows = Vec::with_capacity(entries.len());
        let header: Vec<Value> = columns
            .iter()
            .map(|column| json!({"id":column.id, "name":column.name, "type":column.kind}))
            .collect();
        let mut bytes = json!([header, omitted_columns]).to_string().len();
        for entry in entries {
            if entry["object"] != "page" {
                return Err(failure(Kind::UnsupportedContent).into());
            }
            let row_id = id(&entry["id"])?;
            if !ids.insert(row_id.clone()) {
                return Err(failure(Kind::Internal).into());
            }
            if scope
                .exclusions
                .exclusion(&row_id, SourceType::Page)
                .is_some()
            {
                return Err(denied("row.excluded").into());
            }
            let row = self.fetch_page_value(&PageId(row_id.clone())).await?;
            object(&row, "page", &row_id)?;
            if pages::inactive(&row) != Some(false) {
                return Err(denied("row.inactive").into());
            }
            if row["parent"]["type"] != "data_source_id"
                || id(&row["parent"]["data_source_id"])? != view.source
            {
                return Err(denied("row.not_in_source").into());
            }
            row["last_edited_time"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| failure(Kind::Internal))?;
            let properties = row["properties"]
                .as_object()
                .ok_or_else(|| failure(Kind::Internal))?;
            let mut values = Vec::with_capacity(columns.len());
            let mut omitted = Vec::new();
            for column in &columns {
                let value = properties
                    .get(&column.name)
                    .filter(|p| p["id"] == column.id && p["type"] == column.kind)
                    .and_then(project_value);
                match value {
                    Some(value) => values.push(value),
                    None => {
                        values.push(Value::Null);
                        omitted.push(column.id.clone());
                    }
                }
            }
            let mut entry = json!({"page_id":row_id, "values":values});
            if projection.row_revision {
                entry["row_revision"] = json!(row_revision(&row));
            }
            if !omitted.is_empty() {
                entry["omitted"] = json!(omitted);
            }
            let row = entry;
            bytes += row.to_string().len();
            if bytes > MAX_VIEW_ROW_PAYLOAD_BYTES {
                return Err(BackendError {
                    operation: "notion.view_query.response_limit",
                    ..failure(Kind::UnsupportedContent)
                }
                .into());
            }
            rows.push(row);
        }
        // Before emitting data, reject a reparented or reconfigured view/source.
        let current = self.verified_view(&view_id, scope).await?;
        if revision != current.fingerprint()? {
            return Err(failure(Kind::Conflict).into());
        }
        let status = if truncated {
            "incomplete"
        } else if continuation.is_none() && rows.is_empty() && next.is_null() {
            "empty"
        } else if next.is_null() {
            "complete"
        } else {
            "more"
        };
        let mut output = json!({
            "view_id":view_id, "data_source_id":current.source, "filter":current.value["filter"],
            "sorts":current.value["sorts"], "quick_filters":current.value["quick_filters"], "status":status,
            "columns":header, "rows":rows, "continuation":next
        });
        if !omitted_columns.is_empty() {
            output["omitted_columns"] = json!(omitted_columns);
        }
        Ok(output)
    }
}
