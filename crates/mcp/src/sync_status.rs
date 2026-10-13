//! Read-only operational status; no caller-supplied scope or paths.
use rmcp::model::{Tool, ToolAnnotations};
use serde_json::json;
pub fn tool() -> Tool {
    let input = json!({"type":"object","additionalProperties":false,"properties":{}});
    let nullable_integer = json!({"type":["integer","null"],"minimum":0});
    let output = json!({"type":"object","additionalProperties":false,"required":["last_successful_crawl_unix_seconds","last_successful_reconciliation_unix_seconds","webhooks","operational_schema_version","index"],"properties":{
        "last_successful_crawl_unix_seconds":nullable_integer,
        "last_successful_reconciliation_unix_seconds":nullable_integer,
        "operational_schema_version":nullable_integer,
        "webhooks":{"type":["object","null"],"additionalProperties":false,"required":["pending","running","failed"],"properties":{"pending":{"type":"integer","minimum":0},"running":{"type":"integer","minimum":0},"failed":{"type":"integer","minimum":0}}},
        "index":{"type":["object","null"],"additionalProperties":false,"required":["version","chunk_schema_version","canonical_schema_version","model","model_version","provider"],"properties":{"version":{"type":"integer","minimum":0},"chunk_schema_version":{"type":"integer","enum":[1]},"canonical_schema_version":{"type":"integer","enum":[1]},"model":{"type":"string","enum":["qwen3_embedding06b"]},"model_version":{"type":"string"},"provider":{"type":"string"}}}
    }});
    Tool::new("knowledge_sync_status", "Inspect local synchronization freshness, current scoped webhook states and persisted index/schema/model identity. Null means unconfigured, incompatible or no trustworthy completion evidence. No source requests or index updates are performed.", input.as_object().unwrap().clone())
        .with_raw_output_schema(std::sync::Arc::new(output.as_object().unwrap().clone()))
        .with_annotations(ToolAnnotations::new().read_only(true).destructive(false).open_world(false))
}
