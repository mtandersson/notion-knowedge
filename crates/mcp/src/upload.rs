//! ChatGPT bounded multi-file and legacy single-file parameter schemas.
//! A file reference is input metadata, NOT authorization to fetch its URL.
//! The trusted installed application owns download/SSRF protection and native attachment.

use rmcp::model::{Tool, ToolAnnotations};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatGptFile {
    pub download_url: String,
    pub file_id: String,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadFileRequest {
    pub target: String,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub file: Option<ChatGptFile>,
    #[serde(default)]
    pub files: Option<Vec<ChatGptFile>>,
}

/// Server-owned batch limit; the schema bounds the absolute maximum, and
/// this setting can only tighten it. No client input controls this value.
pub const DEFAULT_MAX_FILES: usize = 4;
pub const HARD_MAX_FILES: usize = 16;

impl std::fmt::Debug for ChatGptFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChatGptFile([REDACTED])")
    }
}
impl std::fmt::Debug for UploadFileRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UploadFileRequest([REDACTED])")
    }
}

pub fn validate_file(file: &ChatGptFile) -> Result<(), &'static str> {
    if file.download_url.trim().is_empty()
        || file.download_url.len() > 8192
        || file.download_url.chars().any(char::is_control)
        || file.file_id.trim().is_empty()
        || file.file_id.len() > 256
        || file.file_id.chars().any(char::is_control)
        || file
            .mime_type
            .as_ref()
            .is_some_and(|s| s.len() > 256 || s.chars().any(char::is_control))
        || file
            .file_name
            .as_ref()
            .is_some_and(|s| s.len() > 512 || s.chars().any(char::is_control))
    {
        return Err("invalid knowledge_upload_file metadata");
    }
    Ok(())
}

impl UploadFileRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.validate_with_limit(DEFAULT_MAX_FILES)
    }

    pub fn validate_with_limit(&self, max_files: usize) -> Result<(), &'static str> {
        if !(1..=HARD_MAX_FILES).contains(&max_files) {
            return Err("invalid knowledge_upload_file batch limit");
        }
        let target = &self.target;
        if target.len() != 36 || !target.bytes().enumerate().all(|(i, ch)| {
            if matches!(i, 8 | 13 | 18 | 23) { ch == b'-' }
            else { ch.is_ascii_digit() || (b'a'..=b'f').contains(&ch) }
        }) || self.caption.as_deref().is_some_and(|value| {
            value.chars().count() > 2000 || value.chars().any(|ch| ch.is_control()
                || matches!(ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
        }) {
            return Err("invalid knowledge_upload_file target or caption");
        }
        match (&self.file, &self.files) {
            (Some(file), None) => validate_file(file),
            (None, Some(files)) if !files.is_empty() && files.len() <= max_files => {
                for file in files {
                    validate_file(file)?;
                }
                Ok(())
            }
            _ => Err("invalid knowledge_upload_file file count"),
        }
    }

    /// Input-order file references for the trusted isolated attachment application.
    /// This parser never opens any URL and never authorizes a Notion write.
    pub fn ordered_files(&self) -> Vec<&ChatGptFile> {
        if let Some(files) = &self.files {
            files.iter().collect()
        } else {
            self.file.iter().collect()
        }
    }
}

pub fn tool() -> Tool {
    tool_with_limit(DEFAULT_MAX_FILES)
}

pub fn tool_with_limit(max_files: usize) -> Tool {
    assert!((1..=HARD_MAX_FILES).contains(&max_files));
    let file = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["download_url", "file_id"],
        "properties": {
            "download_url": {"type": "string"},
            "file_id": {"type": "string"},
            "mime_type": {"type": "string"},
            "file_name": {"type": "string"}
        }
    });
    let input = json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["target"],
        "oneOf": [{"required":["file"]}, {"required":["files"]}],
        "properties": {
            "target": {"type":"string", "pattern":"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$"},
            "caption": {"type":"string", "maxLength":2000},
            "file": file,
            "files": {
                "type": "array",
                "minItems": 1,
                "maxItems": max_files,
                "items": file
            }
        }
    });
    Tool::new(
        "knowledge_upload_file",
        "Attach one or a bounded ordered array of ChatGPT file references to the explicit allowed Notion target as native image/file blocks. Requires configured trusted ingestion; otherwise returns file_upload_unavailable before effects. Reports verified, failed, indeterminate and unattempted per-file outcomes and partial success. Never treat the temporary URL as authorization.",
        input.as_object().expect("static upload schema").clone(),
    )
    .with_annotations(
        ToolAnnotations::new()
            .read_only(false)
            .destructive(false)
            .open_world(true),
    )
    .with_meta(
        serde_json::from_value(json!({"openai/fileParams": ["file","files"]}))
            .expect("static ChatGPT file metadata"),
    )
}

/// Explicit composition-owned installation for authenticated HTTP file requests.
pub trait RequestFileIngestionFactory: Send + Sync {
    fn bind<'a>(
        &'a self,
        adapters: crate::RequestScopeAdapters,
        context: &'a rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<std::sync::Arc<dyn FileIngestion>, rmcp::ErrorData>,
                > + Send
                + 'a,
        >,
    >;
}

/// Trusted application installed by the server composition root.
/// Requests contain untrusted file metadata; implementations retain authority.
pub trait FileIngestion: Send + Sync {
    fn ingest<'a>(
        &'a self,
        request: UploadFileRequest,
        cancellation: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<serde_json::Value, &'static str>> + Send + 'a>,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn item(id: &str) -> serde_json::Value {
        json!({"download_url":"https://files.example.test/file?signature=secret","file_id":id})
    }

    #[test]
    fn schema_preserves_legacy_input_and_adds_bounded_file_array() {
        let schema = serde_json::to_value(tool()).unwrap();
        assert_eq!(
            schema["_meta"]["openai/fileParams"],
            json!(["file", "files"])
        );
        let input = &schema["inputSchema"];
        assert_eq!(
            input["oneOf"],
            json!([{"required":["file"]},{"required":["files"]}])
        );
        assert_eq!(input["properties"]["files"]["minItems"], 1);
        assert_eq!(input["properties"]["files"]["maxItems"], DEFAULT_MAX_FILES);
        assert_eq!(
            input["properties"]["files"]["items"]["required"],
            json!(["download_url", "file_id"])
        );
        assert_eq!(schema["annotations"]["readOnlyHint"], false);
    }

    #[test]
    fn ordered_inputs_and_configurable_limit_are_enforced() {
        let request: UploadFileRequest = serde_json::from_value(json!({
            "target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "files":[item("first"),item("second"),item("third")]
        }))
        .unwrap();
        assert!(request.validate_with_limit(3).is_ok());
        assert!(request.validate_with_limit(2).is_err());
        let ids: Vec<&str> = request
            .ordered_files()
            .iter()
            .map(|file| file.file_id.as_str())
            .collect();
        assert_eq!(ids, vec!["first", "second", "third"]);
        let legacy: UploadFileRequest = serde_json::from_value(
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","file":item("old")}),
        )
        .unwrap();
        assert_eq!(legacy.ordered_files()[0].file_id, "old");
        assert!(legacy.validate_with_limit(1).is_ok());
    }

    #[test]
    fn reject_both_absent_oversize_or_invalid_metadata_without_echoing_url() {
        let invalid = [
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}),
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","file":item("a"),"files":[item("b")]}),
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","files":[]}),
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","files":[item("a"),item("b")]}),
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","files":[{"file_id":"missing-url","download_url":""}]}),
        ];
        for value in invalid {
            let request: UploadFileRequest = serde_json::from_value(value).unwrap();
            assert!(request.validate_with_limit(1).is_err());
        }
        let mut too_many = Vec::new();
        for _ in 0..HARD_MAX_FILES + 1 {
            too_many.push(item("file"));
        }
        let request: UploadFileRequest = serde_json::from_value(
            json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","files":too_many}),
        )
        .unwrap();
        assert!(request.validate_with_limit(HARD_MAX_FILES).is_err());
        let error = request.validate_with_limit(HARD_MAX_FILES).unwrap_err();
        assert!(!error.contains("https://"));
        assert!(
            serde_json::from_value::<UploadFileRequest>(
                json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","files":[item("a")],"authority":"unauthorized-page"})
            )
            .is_err()
        );
    }
    #[test]
    fn request_debug_and_validation_never_disclose_metadata() {
        let mut value = json!({"target":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "caption":"private-sentinel", "file":item("private-sentinel")});
        let request: UploadFileRequest = serde_json::from_value(value.clone()).unwrap();
        assert!(request.validate().is_ok());
        assert_eq!(format!("{request:?}"), "UploadFileRequest([REDACTED])");
        assert_eq!(
            format!("{:?}", request.file),
            "Some(ChatGptFile([REDACTED]))"
        );
        for caption in [
            "x".repeat(2001),
            "private-sentinel\n".into(),
            "private-sentinel\u{202e}".into(),
        ] {
            value["caption"] = json!(caption);
            let request: UploadFileRequest = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(
                request.validate().unwrap_err(),
                "invalid knowledge_upload_file target or caption"
            );
        }
    }
}
