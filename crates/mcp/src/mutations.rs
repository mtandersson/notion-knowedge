//! Explicit trusted semantic mutation capabilities. No server startup or OAuth
//! path installs these; later composition must supply live request authority.
use notion_knowledge_core::{
    append::{AppendError, AppendRequest, append_once_and_refresh},
    audit::{AuditActor, AuditEvent, AuditOutcome, AuditStore, AuditTool},
    backend::{NotionRead, NotionSectionWrite, NotionWrite, PageId},
    create::{CreateError, CreateRequest, create_once_and_refresh},
    destructive::{ConfirmationProvider, ConfirmationRequest, DestructivePolicy, WriteOperation},
    idempotency::{MutationClaim, WriteIdempotencyStore},
    lifecycle::{LifecycleScope, PageLifecycle},
    root_scope::{RootScopeGate, ScopeError},
    section_update::{SectionUpdateError, SectionUpdateRequest, update_section_once_and_refresh},
    sections::SectionAnchor,
    write_refresh::{IndexStatus, MutationRefreshQueue, WriteCompletion},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub trait MutationSource: NotionRead + NotionWrite + NotionSectionWrite + PageLifecycle {}
impl<T: NotionRead + NotionWrite + NotionSectionWrite + PageLifecycle> MutationSource for T {}

/// Build only from trusted composition, never tool JSON. Supplying this does
/// not disable the server's independent read-only/destructive/capability gates.
pub struct MutationRuntime {
    source: Arc<dyn MutationSource>,
    scope: LifecycleScope,
    ledger: Arc<dyn WriteIdempotencyStore>,
    queue: Arc<dyn MutationRefreshQueue>,
    audit: Arc<dyn AuditStore>,
    actor: AuditActor,
    confirmation: Option<Arc<dyn ConfirmationProvider>>,
}
impl MutationRuntime {
    pub fn new(
        source: Arc<dyn MutationSource>,
        scope: LifecycleScope,
        ledger: Arc<dyn WriteIdempotencyStore>,
        queue: Arc<dyn MutationRefreshQueue>,
        audit: Arc<dyn AuditStore>,
        actor: AuditActor,
    ) -> Result<Self, ScopeError> {
        RootScopeGate::new(source.clone(), scope.clone())?;
        Ok(Self {
            source,
            scope,
            ledger,
            queue,
            audit,
            actor,
            confirmation: None,
        })
    }
    pub fn with_confirmation(mut self, provider: Arc<dyn ConfirmationProvider>) -> Self {
        self.confirmation = Some(provider);
        self
    }
    pub(crate) fn supports(&self, name: &str) -> bool {
        matches!(name, "knowledge_create_page" | "knowledge_append")
            || (name == "knowledge_update_section" && self.confirmation.is_some())
    }
    pub(crate) fn tools(&self) -> Vec<rmcp::model::Tool> {
        [
            "knowledge_create_page",
            "knowledge_append",
            "knowledge_update_section",
        ]
        .into_iter()
        .filter(|name| self.supports(name))
        .map(tool)
        .collect()
    }
    pub(crate) async fn call(
        &self,
        name: &str,
        args: serde_json::Map<String, Value>,
        cancel: CancellationToken,
    ) -> Result<rmcp::model::CallToolResult, rmcp::ErrorData> {
        let input = Input::parse(name, args)?;
        if cancel.is_cancelled() {
            return Ok(failure("cancelled", false));
        }
        let mut audit =
            AuditAttempt::begin(self, input.audit_tool(), input.target()).map_err(|_| {
                rmcp::ErrorData::internal_error(
                    "mutation audit unavailable; no mutation attempted",
                    None,
                )
            })?;
        let outcome = tokio::select! {
            biased;
            _=cancel.cancelled()=>Err(Failure::Unknown),
            result=self.execute(input)=>result,
        };
        let audit_outcome = match &outcome {
            Ok(_) => AuditOutcome::Succeeded,
            Err(Failure::Denied) => AuditOutcome::Denied,
            Err(Failure::Unknown) => AuditOutcome::Indeterminate,
            Err(_) => AuditOutcome::Failed,
        };
        let audited = audit.finish(
            audit_outcome,
            outcome.as_ref().ok().map(|r| r.receipt.page_id.as_str()),
        );
        Ok(match outcome {
            Ok(result) => {
                let index = match result.index {
                    IndexStatus::Queued => "queued",
                    IndexStatus::Running => "running",
                    IndexStatus::Completed => "completed",
                    IndexStatus::Failed => "failed",
                    IndexStatus::Unavailable => "unavailable",
                };
                let value = json!({"write":{"status":"verified","page_id":result.receipt.page_id,"url":result.receipt.url,"last_edited_time":result.receipt.last_edited_time,"replayed":result.replayed},"index":{"status":index,"refresh_id":result.refresh.mutation_digest},"audit":{"status":if audited {"recorded"} else {"unavailable"}}});
                let mut response = rmcp::model::CallToolResult::structured(value);
                response.is_error = Some(!audited);
                response
            }
            Err(error) => failure(error.code(), audited),
        })
    }
    async fn execute(&self, input: Input) -> Result<WriteCompletion, Failure> {
        if !self.scope.roots.iter().any(|root| root.0 == input.root()) {
            return Err(Failure::Denied);
        }
        // Caller choice can only narrow trusted roots, never add authority.
        let mut scope = self.scope.clone();
        scope.roots = vec![PageId(input.root().to_owned())];
        let gate =
            RootScopeGate::new(self.source.clone(), scope.clone()).map_err(|_| Failure::Denied)?;
        match input {
            Input::Create(input) => create_once_and_refresh(
                self.source.as_ref(),
                self.source.as_ref(),
                &gate,
                self.ledger.as_ref(),
                self.queue.as_ref(),
                CreateRequest {
                    parent_page_id: PageId(input.parent_page_id),
                    title: input.title,
                    markdown: input.markdown,
                    idempotency_key: input.idempotency_key,
                },
            )
            .await
            .map_err(Failure::from),
            Input::Append(input) => append_once_and_refresh(
                self.source.as_ref(),
                self.source.as_ref(),
                &gate,
                self.ledger.as_ref(),
                self.queue.as_ref(),
                AppendRequest {
                    page_id: PageId(input.page_id),
                    markdown: input.markdown,
                    expected_last_edited_time: input.expected_last_edited_time,
                    expected_markdown_sha256: input.expected_markdown_sha256,
                    idempotency_key: input.idempotency_key,
                },
            )
            .await
            .map_err(Failure::from),
            Input::Section(input) => {
                gate.authorize(&PageId(input.page_id.clone()))
                    .await
                    .map_err(|_| Failure::Denied)?;
                let digest = MutationClaim::payload_sha256(
                    &serde_json::to_vec(&input).map_err(|_| Failure::Invalid)?,
                );
                let trusted_scope = format!(
                    "{}:{}:{}",
                    scope.workspace_id, scope.generation, input.root_page_id
                );
                let request = ConfirmationRequest::new(
                    WriteOperation::UpdateSection,
                    &trusted_scope,
                    &input.page_id,
                    &digest,
                    &input.idempotency_key,
                )
                .map_err(|_| Failure::Invalid)?;
                DestructivePolicy::new(true)
                    .require_confirmation(&request, self.confirmation.as_deref())
                    .await
                    .map_err(|_| Failure::Denied)?
                    .consume(&request)
                    .map_err(|_| Failure::Denied)?;
                update_section_once_and_refresh(
                    self.source.as_ref(),
                    self.source.as_ref(),
                    &gate,
                    self.ledger.as_ref(),
                    self.queue.as_ref(),
                    SectionUpdateRequest {
                        page_id: PageId(input.page_id),
                        anchor: SectionAnchor {
                            level: input.section_anchor.level,
                            heading: input.section_anchor.heading,
                        },
                        replacement: input.markdown,
                        expected_last_edited_time: input.expected_last_edited_time,
                        expected_markdown_sha256: input.expected_markdown_sha256,
                        idempotency_key: input.idempotency_key,
                    },
                )
                .await
                .map_err(Failure::from)
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CreateInput {
    root_page_id: String,
    parent_page_id: String,
    title: String,
    markdown: String,
    idempotency_key: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AppendInput {
    root_page_id: String,
    page_id: String,
    markdown: String,
    idempotency_key: String,
    expected_last_edited_time: Option<String>,
    expected_markdown_sha256: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Anchor {
    level: u8,
    heading: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SectionInput {
    root_page_id: String,
    page_id: String,
    markdown: String,
    idempotency_key: String,
    section_anchor: Anchor,
    expected_last_edited_time: Option<String>,
    expected_markdown_sha256: Option<String>,
}
enum Input {
    Create(CreateInput),
    Append(AppendInput),
    Section(SectionInput),
}
fn canonical(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == b'-'
            } else {
                c.is_ascii_digit() || (b'a'..=b'f').contains(&c)
            }
        })
}
impl Input {
    fn parse(name: &str, args: serde_json::Map<String, Value>) -> Result<Self, rmcp::ErrorData> {
        let value = Value::Object(args);
        let bad = || rmcp::ErrorData::invalid_params("invalid semantic mutation arguments", None);
        let input = match name {
            "knowledge_create_page" => {
                Self::Create(serde_json::from_value(value).map_err(|_| bad())?)
            }
            "knowledge_append" => Self::Append(serde_json::from_value(value).map_err(|_| bad())?),
            "knowledge_update_section" => {
                Self::Section(serde_json::from_value(value).map_err(|_| bad())?)
            }
            _ => return Err(bad()),
        };
        if !canonical(input.root()) || !canonical(input.target()) {
            return Err(bad());
        }
        Ok(input)
    }
    fn root(&self) -> &str {
        match self {
            Self::Create(q) => &q.root_page_id,
            Self::Append(q) => &q.root_page_id,
            Self::Section(q) => &q.root_page_id,
        }
    }
    fn target(&self) -> &str {
        match self {
            Self::Create(q) => &q.parent_page_id,
            Self::Append(q) => &q.page_id,
            Self::Section(q) => &q.page_id,
        }
    }
    fn audit_tool(&self) -> AuditTool {
        match self {
            Self::Create(_) => AuditTool::PageCreate,
            Self::Append(_) => AuditTool::PageAppend,
            Self::Section(_) => AuditTool::PageReplace,
        }
    }
}
enum Failure {
    Invalid,
    Denied,
    Conflict,
    Idempotency,
    Unavailable,
    Unknown,
}
impl Failure {
    fn code(&self) -> &'static str {
        match self {
            Self::Invalid => "invalid_input",
            Self::Denied => "not_authorized",
            Self::Conflict => "revision_conflict",
            Self::Idempotency => "idempotency_conflict",
            Self::Unavailable => "unavailable",
            Self::Unknown => "outcome_unknown",
        }
    }
}
impl From<CreateError> for Failure {
    fn from(e: CreateError) -> Self {
        match e {
            CreateError::InvalidInput => Self::Invalid,
            CreateError::NotAuthorized => Self::Denied,
            CreateError::IdempotencyConflict => Self::Idempotency,
            CreateError::Unavailable => Self::Unavailable,
            CreateError::OutcomeUnknown => Self::Unknown,
        }
    }
}
impl From<AppendError> for Failure {
    fn from(e: AppendError) -> Self {
        match e {
            AppendError::InvalidInput => Self::Invalid,
            AppendError::NotAuthorized => Self::Denied,
            AppendError::RevisionConflict(_) => Self::Conflict,
            AppendError::IdempotencyConflict => Self::Idempotency,
            AppendError::Unavailable => Self::Unavailable,
            AppendError::OutcomeUnknown => Self::Unknown,
        }
    }
}
impl From<SectionUpdateError> for Failure {
    fn from(e: SectionUpdateError) -> Self {
        match e {
            SectionUpdateError::InvalidInput | SectionUpdateError::SelectionRejected => {
                Self::Invalid
            }
            SectionUpdateError::NotAuthorized => Self::Denied,
            SectionUpdateError::RevisionConflict(_) => Self::Conflict,
            SectionUpdateError::IdempotencyConflict => Self::Idempotency,
            SectionUpdateError::Unavailable => Self::Unavailable,
            SectionUpdateError::OutcomeUnknown => Self::Unknown,
        }
    }
}
fn failure(code: &str, audited: bool) -> rmcp::model::CallToolResult {
    let mut output = rmcp::model::CallToolResult::structured(
        json!({"write":{"status":code},"index":{"status":"not_queued"},"audit":{"status":if audited {"recorded"} else {"unavailable"}}}),
    );
    output.is_error = Some(true);
    output
}
fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
struct AuditAttempt<'a> {
    runtime: &'a MutationRuntime,
    tool: AuditTool,
    target: String,
    id: String,
    finished: bool,
}
impl<'a> AuditAttempt<'a> {
    fn begin(runtime: &'a MutationRuntime, tool: AuditTool, target: &str) -> Result<Self, ()> {
        let mut attempt = Self {
            runtime,
            tool,
            target: target.to_owned(),
            id: notion_knowledge_core::logging::current_id()
                .as_str()
                .to_owned(),
            finished: false,
        };
        if !attempt.record(AuditOutcome::Attempted, &attempt.target) {
            attempt.finished = true;
            return Err(());
        }
        Ok(attempt)
    }
    fn record(&self, outcome: AuditOutcome, target: &str) -> bool {
        let time = now();
        AuditEvent::new(
            time,
            self.runtime.actor,
            self.tool,
            Some(target.to_owned()),
            outcome,
            self.id.clone(),
            None,
        )
        .is_ok_and(|e| self.runtime.audit.record(&e, time).is_ok())
    }
    fn finish(&mut self, outcome: AuditOutcome, target: Option<&str>) -> bool {
        let recorded = self.record(outcome, target.unwrap_or(&self.target));
        self.finished = true;
        recorded
    }
}
impl Drop for AuditAttempt<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.record(AuditOutcome::Indeterminate, &self.target);
        }
    }
}

pub(crate) fn tool(name: &str) -> rmcp::model::Tool {
    let mut tool = crate::write_contract::tool(name).expect("implemented semantic mutation");
    tool.description=Some(match name {"knowledge_create_page"=>"Create one page under an explicitly authorized parent with once-only idempotency, authoritative read-back and separate local refresh status.","knowledge_append"=>"Append exact Markdown at the end of one authorized page with fresh revision guards, once-only idempotency and separate local refresh status.",_=>"Update one exact heading's body with fresh revision guards, trusted server confirmation, once-only idempotency and separate local refresh status."}.into());
    if name == "knowledge_update_section" {
        let mut schema = tool.input_schema.as_ref().clone();
        let properties = schema
            .get_mut("properties")
            .unwrap()
            .as_object_mut()
            .unwrap();
        properties.remove("confirmation_id");
        properties.insert("section_anchor".into(),json!({"type":"object","additionalProperties":false,"required":["level","heading"],"properties":{"level":{"type":"integer","minimum":1,"maximum":6},"heading":{"type":"string","minLength":1,"maxLength":200}}}));
        properties.insert(
            "markdown".into(),
            json!({"type":"string","minLength":0,"maxLength":200000}),
        );
        if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
            required.retain(|v| v != "confirmation_id");
        }
        tool.input_schema = Arc::new(schema);
    }
    tool.output_schema=Some(Arc::new(json!({"type":"object","required":["write","index","audit"],"properties":{"write":{"type":"object"},"index":{"type":"object"},"audit":{"type":"object"}},"additionalProperties":false}).as_object().unwrap().clone()));
    tool
}
