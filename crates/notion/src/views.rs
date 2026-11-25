//! Authoritative saved-view execution and physical container authorization.
use std::collections::HashSet;

use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind as Kind, PageId},
    discovery::SourceType,
    lifecycle::LifecycleScope,
};
use reqwest::Method;
use serde_json::{Value, json};

use crate::{NotionClient, pages};

fn failure(kind: Kind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.view_query",
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

struct View {
    value: Value,
    database: String,
    source: String,
    source_database: String,
}

impl NotionClient {
    async fn view_object(&self, kind: &str, id: &str) -> Result<Value, BackendError> {
        self.walk_request(Method::GET, &format!("{kind}/{id}"), None)
            .await
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
                _ => return Err(failure(Kind::PermissionDenied)),
            };
            if !visited.insert((kind, current.clone()))
                || source_type
                    .is_some_and(|source| scope.exclusions.exclusion(&current, source).is_some())
                || (kind == "page_id" && scope.exclusions.descendants_of.contains(&current))
            {
                return Err(failure(Kind::PermissionDenied));
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
                        return Err(failure(Kind::PermissionDenied));
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
                (_, Some(true)) | ("database_id" | "page_id" | "block_id", None) => {
                    return Err(failure(Kind::PermissionDenied));
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
                _ => return Err(failure(Kind::PermissionDenied)),
            };
            current = id(&parent[kind])?;
        }
        Err(failure(Kind::PermissionDenied))
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
        if scope
            .exclusions
            .exclusion(&source, SourceType::DataSource)
            .is_some()
        {
            return Err(failure(Kind::PermissionDenied));
        }
        let metadata = self.view_object("data_sources", &source).await?;
        object(&metadata, "data_source", &source)?;
        if metadata["parent"]["type"] != "database_id" || pages::inactive(&metadata) == Some(true) {
            return Err(failure(Kind::PermissionDenied));
        }
        let source_database = id(&metadata["parent"]["database_id"])?;
        self.verify_database_scope(&source_database, false, scope)
            .await?;
        if source_database != database {
            self.verify_database_scope(&database, true, scope).await?;
        }
        Ok(View {
            value,
            database,
            source,
            source_database,
        })
    }

    pub async fn query_saved_view(
        &self,
        scope: &LifecycleScope,
        reference: &str,
        page_size: usize,
        continuation: Option<(&str, &str, &str)>,
    ) -> Result<Value, BackendError> {
        if !(1..=25).contains(&page_size) {
            return Err(failure(Kind::InvalidInput));
        }
        let view_id = pages::uuid(reference.strip_prefix("view://").unwrap_or(reference))
            .ok_or_else(|| failure(Kind::InvalidInput))?;
        let view = self.verified_view(&view_id, scope).await?;
        let revision = view.value["last_edited_time"]
            .as_str()
            .filter(|time| !time.is_empty() && time.len() <= 64)
            .ok_or_else(|| failure(Kind::Internal))?;
        let path = format!("views/{view_id}/queries");
        let (response, query_id) = match continuation {
            Some((query, cursor, expected_revision)) => {
                if revision != expected_revision {
                    return Err(failure(Kind::Conflict));
                }
                if cursor.is_empty() || cursor.len() > 256 || cursor.chars().any(char::is_control) {
                    return Err(failure(Kind::InvalidInput));
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
                    return Err(failure(Kind::Internal));
                }
                (response, query)
            }
            None => {
                let response = self
                    .walk_request(Method::POST, &path, Some(&json!({"page_size":page_size})))
                    .await?;
                if response["object"] != "view_query" || id(&response["view_id"])? != view_id {
                    return Err(failure(Kind::Internal));
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
            _ => return Err(failure(Kind::Internal)),
        };
        let truncated = match response["request_status"]["type"].as_str() {
            Some("incomplete") => true,
            Some("complete") => false,
            None if response["request_status"].is_null() => false,
            _ => return Err(failure(Kind::Internal)),
        };
        // Query results carry references. Resolve each row in result order and
        // revalidate its active physical source before exposing its properties.
        let mut ids = HashSet::new();
        let mut rows = Vec::with_capacity(entries.len());
        for entry in entries {
            if entry["object"] != "page" {
                return Err(failure(Kind::UnsupportedContent));
            }
            let row_id = id(&entry["id"])?;
            if !ids.insert(row_id.clone()) {
                return Err(failure(Kind::Internal));
            }
            if scope
                .exclusions
                .exclusion(&row_id, SourceType::Page)
                .is_some()
            {
                return Err(failure(Kind::PermissionDenied));
            }
            let mut row = self.fetch_page_value(&PageId(row_id.clone())).await?;
            object(&row, "page", &row_id)?;
            if pages::inactive(&row) != Some(false)
                || row["parent"]["type"] != "data_source_id"
                || id(&row["parent"]["data_source_id"])? != view.source
            {
                return Err(failure(Kind::PermissionDenied));
            }
            let revision = row["last_edited_time"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| failure(Kind::Internal))?
                .to_owned();
            let mut calls = 0;
            self.complete_properties(&PageId(row_id.clone()), &mut row, &mut calls)
                .await?;
            if calls != 0 {
                let current = self.fetch_page_value(&PageId(row_id.clone())).await?;
                object(&current, "page", &row_id)?;
                if pages::inactive(&current) != Some(false)
                    || row["parent"] != current["parent"]
                    || current["last_edited_time"] != revision
                {
                    return Err(failure(Kind::Conflict));
                }
            }
            let properties = row["properties"]
                .as_object_mut()
                .ok_or_else(|| failure(Kind::Internal))?;
            for property in properties.values_mut() {
                let fields = property
                    .as_object_mut()
                    .ok_or_else(|| failure(Kind::Internal))?;
                fields.remove(pages::PAGINATED);
            }
            rows.push(
                json!({"page_id":row_id,"last_edited_time":revision,"properties":properties}),
            );
        }
        // Before emitting data, reject a reparented or reconfigured view/source.
        let current = self.verified_view(&view_id, scope).await?;
        if view.database != current.database
            || view.source != current.source
            || view.source_database != current.source_database
            || view.value["last_edited_time"] != current.value["last_edited_time"]
            || view.value["filter"] != current.value["filter"]
            || view.value["sorts"] != current.value["sorts"]
        {
            return Err(failure(Kind::Conflict));
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
        Ok(json!({
            "view_id":view_id, "data_source_id":current.source, "filter":current.value["filter"],
            "sorts":current.value["sorts"], "status":status, "rows":rows, "continuation":next
        }))
    }
}
