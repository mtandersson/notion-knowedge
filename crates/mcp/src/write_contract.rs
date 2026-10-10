//! Contract-only semantic write catalog for #76.
//!
//! Preview is opt-in and has no mutation backend. Production discovery remains
//! read-only until the independently reviewed workflow/auth tickets ship.
use rmcp::model::{Tool, ToolAnnotations};
use serde_json::{Map, Value, json};
use std::sync::Arc;

const NAMES: [&str; 4] = [
    "knowledge_create_page",
    "knowledge_append",
    "knowledge_update_section",
    "knowledge_archive_page",
];

pub fn is_planned_write(name: &str) -> bool {
    NAMES.contains(&name)
}

fn short_id() -> Value {
    json!({"type":"string","minLength":1,"maxLength":128,"pattern":"\\S"})
}

fn content() -> Value {
    json!({"type":"string","minLength":1,"maxLength":200000,"pattern":"\\S"})
}

fn input(name: &str) -> Value {
    let mut fields = Map::new();
    fields.insert("root_page_id".into(), short_id());
    fields.insert("idempotency_key".into(), short_id());
    let mut required = vec!["root_page_id", "idempotency_key"];

    match name {
        "knowledge_create_page" => {
            fields.insert("parent_page_id".into(), short_id());
            fields.insert(
                "title".into(),
                json!({"type":"string","minLength":1,"maxLength":200,"pattern":"\\S"}),
            );
            fields.insert("markdown".into(), content());
            required.extend(["parent_page_id", "title", "markdown"]);
        }
        "knowledge_append" | "knowledge_update_section" => {
            fields.insert("page_id".into(), short_id());
            fields.insert("markdown".into(), content());
            fields.insert(
                "expected_last_edited_time".into(),
                json!({"type":"string","minLength":1,"maxLength":128,"format":"date-time"}),
            );
            required.extend(["page_id", "markdown", "expected_last_edited_time"]);
            if name == "knowledge_update_section" {
                fields.insert("section_anchor".into(), short_id());
                required.push("section_anchor");
            }
        }
        "knowledge_archive_page" => {
            fields.insert("page_id".into(), short_id());
            fields.insert(
                "expected_last_edited_time".into(),
                json!({"type":"string","minLength":1,"maxLength":128,"format":"date-time"}),
            );
            fields.insert("confirmation_id".into(), short_id());
            required.extend(["page_id", "expected_last_edited_time", "confirmation_id"]);
        }
        _ => unreachable!("only canonical proposed names enter this builder"),
    }
    json!({"type":"object","additionalProperties":false,"required":required,"properties":fields})
}

fn output() -> Value {
    json!({"type":"object","additionalProperties":false,
    "required":["page_id","url","last_edited_time","verified"],
    "properties":{
        "page_id":short_id(),
        "url":{"type":"string","minLength":1},
        "last_edited_time":{"type":"string","minLength":1},
        "verified":{"const":true}
    }})
}

pub fn tool(name: &str) -> Option<Tool> {
    if !is_planned_write(name) {
        return None;
    }
    let (description, destructive) = match name {
        "knowledge_create_page" => (
            "DESIGN PREVIEW ONLY: create one page under an explicitly authorized parent; requires idempotency and server-controlled root/provenance verification. Unavailable until the authenticated create workflow ships; does not mutate Notion.",
            false,
        ),
        "knowledge_append" => (
            "DESIGN PREVIEW ONLY: append Markdown to one authorized page, with source revision precondition and idempotency key; never replace content. Unavailable until the guarded append workflow ships.",
            false,
        ),
        "knowledge_update_section" => (
            "DESIGN PREVIEW ONLY: replace only a named section anchored on one authorized page, guarded by source revision and idempotency. Not a whole-page update or delete; unavailable until implementation ships.",
            true,
        ),
        "knowledge_archive_page" => (
            "DESIGN PREVIEW ONLY: explicitly destructive archive operation on one authorized page. Requires server-issued confirmation, source revision and idempotency; never hidden in generic update. Unavailable until confirmation workflow ships.",
            true,
        ),
        _ => unreachable!(),
    };
    let schema = input(name);
    let out = output();
    Some(
        Tool::new(
            name.to_owned(),
            description,
            schema.as_object().unwrap().clone(),
        )
        .with_raw_output_schema(Arc::new(out.as_object().unwrap().clone()))
        .with_annotations(
            ToolAnnotations::new()
                .read_only(false)
                .destructive(destructive)
                .open_world(true),
        ),
    )
}

pub fn tools() -> Vec<Tool> {
    NAMES
        .iter()
        .map(|name| tool(name).expect("canonical name"))
        .collect()
}
