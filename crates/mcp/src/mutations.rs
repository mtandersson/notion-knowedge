//! Trusted semantic mutation capabilities. The integration composition supplies
//! durable stores and explicit operator activation.
use notion_knowledge_core::{
    append::{AppendError, AppendRequest, append_once_and_refresh},
    audit::{AuditActor, AuditEvent, AuditOutcome, AuditStore, AuditTool},
    backend::{NotionRead, NotionSectionWrite, NotionWrite, PageId, WriteRejection},
    create::{CreateError, CreateRequest, create_once_and_refresh},
    destructive::ConfirmationProvider,
    idempotency::{ClaimRelease, WriteIdempotencyStore},
    lifecycle::LifecycleScope,
    revision::RevisionMetadata,
    root_scope::{ScopeError, validate_scope},
    row_write::{CreateRowRequest, RowWrite, UpdateRowError, UpdateRowRequest},
    section_update::{
        SectionUpdateError, SectionUpdateRequest, update_section_confirmed_once_and_refresh,
    },
    sections::SectionAnchor,
    write_refresh::{IndexStatus, MutationRefreshQueue, WriteCompletion},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};
use tokio_util::sync::CancellationToken;

/// Short field description; the full guidance is returned when a root is rejected.
pub(crate) const ROOT_FIELD: &str = "Root page UUID (not the destination): sources[].provenance.root_page_id from knowledge_get on an indexed page under it.";
pub(crate) const ROOT_GUIDANCE: &str = "Use sources[].provenance.root_page_id from knowledge_get on an indexed page under the intended root. Use the destination page for that lookup when it is indexed. Supply the destination separately as parent_page_id for create or page_id for append or section updates.";

pub trait MutationSource: NotionRead + NotionWrite + NotionSectionWrite {}
impl<T: NotionRead + NotionWrite + NotionSectionWrite> MutationSource for T {}

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
    row_writer: Option<Arc<dyn RowWrite>>,
    page_writes: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
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
        validate_scope(&scope)?;
        Ok(Self {
            source,
            scope,
            ledger,
            queue,
            audit,
            actor,
            confirmation: None,
            row_writer: None,
            page_writes: Mutex::new(HashMap::new()),
        })
    }
    pub fn with_confirmation(mut self, provider: Arc<dyn ConfirmationProvider>) -> Self {
        self.confirmation = Some(provider);
        self
    }
    pub fn with_row_writer(mut self, source: Arc<dyn RowWrite>) -> Self {
        self.row_writer = Some(source);
        self
    }

    fn page_lock(&self, page: &str) -> Result<Arc<tokio::sync::Mutex<()>>, Failure> {
        let mut locks = self.page_writes.lock().map_err(|_| Failure::Unavailable)?;
        locks.retain(|_, lock| lock.strong_count() != 0);
        if let Some(lock) = locks.get(page).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(page.to_owned(), Arc::downgrade(&lock));
        Ok(lock)
    }

    pub(crate) fn supports(&self, name: &str) -> bool {
        matches!(name, "knowledge_create_page" | "knowledge_update_page")
            || (name == "knowledge_update_section" && self.confirmation.is_some())
    }
    /// The listed write tools. Each operation is offered only when policy
    /// permits its own selector (`knowledge_append`, `knowledge_create_row`,
    /// `knowledge_update_row`); those selectors are not tool names.
    pub(crate) fn tools(&self, permits: &dyn Fn(&str) -> bool) -> Vec<rmcp::model::Tool> {
        let rows_create = self.row_writer.is_some() && permits(crate::row_write::CREATE_SELECTOR);
        let append = permits("knowledge_append");
        let patch = self.row_writer.is_some() && permits(crate::row_write::UPDATE_SELECTOR);
        let page_create = permits("knowledge_create_page");
        let mut tools = Vec::new();
        if page_create || rows_create {
            tools.push(with_output(merged_create_tool(page_create, rows_create)));
        }
        if append || patch {
            tools.push(with_output(merged_update_tool(append, patch)));
        }
        if self.supports("knowledge_update_section") {
            tools.push(tool("knowledge_update_section"));
        }
        tools
    }
    pub(crate) async fn call(
        &self,
        name: &str,
        args: serde_json::Map<String, Value>,
        cancel: CancellationToken,
    ) -> Result<rmcp::model::CallToolResult, rmcp::ErrorData> {
        let input = Input::parse(name, args)?;
        if cancel.is_cancelled() {
            return Ok(failure_result(json!({"status":"cancelled"}), false));
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
            _=cancel.cancelled()=>{
                // The caller went away (cancel notification or disconnect)
                // while the write was in flight; the effect may still exist.
                notion_knowledge_core::logging::emit_write_outcome_unknown(
                    notion_knowledge_core::logging::Outcome::Rejected,
                    "mutation.caller_cancelled",
                    "",
                );
                Err(Failure::Unknown)
            }
            result=self.execute(input)=>result,
        };
        let audit_outcome = match &outcome {
            Ok(_) => AuditOutcome::Succeeded,
            Err(Failure::Denied | Failure::RootNotConfigured) => AuditOutcome::Denied,
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
            Err(error) => failure_result(error.write_result(), audited),
        })
    }
    async fn execute(&self, input: Input) -> Result<WriteCompletion, Failure> {
        if !self.scope.roots.iter().any(|root| root.0 == input.root()) {
            return Err(Failure::RootNotConfigured);
        }
        // Caller choice can only narrow trusted roots, never add authority.
        let mut scope = self.scope.clone();
        scope.roots = vec![PageId(input.root().to_owned())];
        let _page_guard = if matches!(&input, Input::UpdateRow(_)) {
            Some(self.page_lock(input.target())?.lock_owned().await)
        } else {
            None
        };
        match input {
            Input::Create(input) => create_once_and_refresh(
                self.source.as_ref(),
                self.source.as_ref(),
                &scope,
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
            Input::CreateRow(input) => self
                .row_writer
                .as_ref()
                .ok_or(Failure::Denied)?
                .create_row(input, &scope, self.ledger.as_ref(), self.queue.as_ref())
                .await
                .map_err(Failure::from),
            Input::UpdateRow(input) => self
                .row_writer
                .as_ref()
                .ok_or(Failure::Denied)?
                .update_row(input, &scope, self.ledger.as_ref(), self.queue.as_ref())
                .await
                .map_err(|error| match error {
                    UpdateRowError::Write(error) => Failure::from(error),
                    UpdateRowError::RevisionConflict(meta) => Failure::Conflict(meta),
                }),
            Input::Append(input) => append_once_and_refresh(
                self.source.as_ref(),
                self.source.as_ref(),
                &scope,
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
            Input::Section(input) => update_section_confirmed_once_and_refresh(
                self.source.as_ref(),
                self.source.as_ref(),
                &scope,
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
                self.confirmation.as_deref().ok_or(Failure::Denied)?,
            )
            .await
            .map_err(Failure::from),
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
    CreateRow(CreateRowRequest),
    UpdateRow(UpdateRowRequest),
    Append(AppendInput),
    Section(SectionInput),
}
fn args_has(value: &Value, key: &str) -> bool {
    value.get(key).is_some()
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
            "knowledge_create_page" if args_has(&value, "data_source_id") => {
                Self::CreateRow(serde_json::from_value(value).map_err(|_| bad())?)
            }
            "knowledge_create_page" => {
                Self::Create(serde_json::from_value(value).map_err(|_| bad())?)
            }
            "knowledge_update_page" => {
                let mut value = value;
                let operation = value
                    .as_object_mut()
                    .and_then(|map| map.remove("operation"))
                    .and_then(|operation| operation.as_str().map(str::to_owned));
                match operation.as_deref() {
                    Some("append_markdown") => {
                        Self::Append(serde_json::from_value(value).map_err(|_| bad())?)
                    }
                    Some("patch_properties") => {
                        Self::UpdateRow(serde_json::from_value(value).map_err(|_| bad())?)
                    }
                    _ => return Err(bad()),
                }
            }
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
            Self::CreateRow(q) => &q.root_page_id,
            Self::UpdateRow(q) => &q.root_page_id,
            Self::Append(q) => &q.root_page_id,
            Self::Section(q) => &q.root_page_id,
        }
    }
    fn target(&self) -> &str {
        match self {
            Self::Create(q) => &q.parent_page_id,
            Self::CreateRow(q) => &q.data_source_id,
            Self::UpdateRow(q) => &q.page_id,
            Self::Append(q) => &q.page_id,
            Self::Section(q) => &q.page_id,
        }
    }
    fn audit_tool(&self) -> AuditTool {
        match self {
            Self::Create(_) | Self::CreateRow(_) => AuditTool::PageCreate,
            Self::UpdateRow(_) => AuditTool::PageReplace,
            Self::Append(_) => AuditTool::PageAppend,
            Self::Section(_) => AuditTool::PageReplace,
        }
    }
}
enum Failure {
    Invalid,
    Denied,
    RootNotConfigured,
    Conflict(RevisionMetadata),
    Idempotency,
    Unavailable,
    /// Notion definitely refused the request; nothing changed and the key was released.
    Rejected(WriteRejection, Effect, ClaimRelease),
    Unknown,
}
/// Which kind of effect Notion refused, for the caller's guidance.
#[derive(Clone, Copy)]
enum Effect {
    Create,
    Change,
}
/// Fixed guidance for a refused write: no ids, titles or upstream text.
fn rejection_message(rejection: WriteRejection, effect: Effect, release: ClaimRelease) -> String {
    let refusal = match (effect, rejection) {
        (Effect::Create, WriteRejection::ParentIsDatabase) => {
            "parent_page_id names a Notion database, not a page. Nothing was created. To add a row, call knowledge_create_page with the database's data_source_id and properties instead of parent_page_id."
        }
        (Effect::Create, WriteRejection::NotFound) => {
            "Notion found no parent page with that id shared with the integration. Nothing was created. If the parent is a database, pass its data_source_id instead of parent_page_id."
        }
        (Effect::Create, WriteRejection::PermissionDenied) => {
            "Notion denied the integration access to the parent page. Nothing was created; share the parent with the integration first."
        }
        (Effect::Create, WriteRejection::InvalidRequest) => {
            "Notion rejected the create request as invalid. Nothing was created; correct the request first."
        }
        (Effect::Change, WriteRejection::NotFound | WriteRejection::ParentIsDatabase) => {
            "Notion found no page with that id shared with the integration. Nothing was changed."
        }
        (Effect::Change, WriteRejection::PermissionDenied) => {
            "Notion denied the integration access to the page. Nothing was changed; share the page with the integration first."
        }
        (Effect::Change, WriteRejection::InvalidRequest) => {
            "Notion rejected the change as invalid. Nothing was changed; correct the request first."
        }
    };
    let retry = match release {
        ClaimRelease::Released => "The same idempotency key can be retried.",
        ClaimRelease::Retained => "This idempotency key stays reserved; retry with a new key.",
    };
    format!("{refusal} {retry}")
}
impl Failure {
    fn code(&self) -> &'static str {
        match self {
            Self::Invalid => "invalid_input",
            Self::Rejected(
                WriteRejection::InvalidRequest | WriteRejection::ParentIsDatabase,
                ..,
            ) => "invalid_input",
            Self::Rejected(WriteRejection::NotFound | WriteRejection::PermissionDenied, ..) => {
                "not_authorized"
            }
            Self::Denied | Self::RootNotConfigured => "not_authorized",
            Self::Conflict(_) => "revision_conflict",
            Self::Idempotency => "idempotency_conflict",
            Self::Unavailable => "unavailable",
            Self::Unknown => "outcome_unknown",
        }
    }

    fn write_result(&self) -> Value {
        let mut write = json!({"status":self.code()});
        match self {
            Self::RootNotConfigured => {
                write["reason"] = json!("root_not_configured");
                write["message"] = json!(ROOT_GUIDANCE);
            }
            Self::Rejected(rejection, effect, release) => {
                write["reason"] = json!(rejection.label());
                write["message"] = json!(rejection_message(*rejection, *effect, *release));
            }
            Self::Conflict(meta) => {
                write["page_id"] = json!(meta.page_id.0);
                write["current_last_edited_time"] = json!(meta.last_edited_time);
            }
            _ => {}
        }
        write
    }
}
impl From<CreateError> for Failure {
    fn from(e: CreateError) -> Self {
        match e {
            CreateError::InvalidInput => Self::Invalid,
            CreateError::NotAuthorized => Self::Denied,
            CreateError::IdempotencyConflict => Self::Idempotency,
            CreateError::Unavailable => Self::Unavailable,
            CreateError::Rejected(rejection, release) => {
                Self::Rejected(rejection, Effect::Create, release)
            }
            CreateError::OutcomeUnknown => Self::Unknown,
        }
    }
}
impl From<AppendError> for Failure {
    fn from(e: AppendError) -> Self {
        match e {
            AppendError::InvalidInput => Self::Invalid,
            AppendError::NotAuthorized => Self::Denied,
            AppendError::RevisionConflict(meta) => Self::Conflict(meta),
            AppendError::IdempotencyConflict => Self::Idempotency,
            AppendError::Unavailable => Self::Unavailable,
            AppendError::Rejected(rejection, release) => {
                Self::Rejected(rejection, Effect::Change, release)
            }
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
            SectionUpdateError::RevisionConflict(meta) => Self::Conflict(meta),
            SectionUpdateError::IdempotencyConflict => Self::Idempotency,
            SectionUpdateError::Unavailable => Self::Unavailable,
            SectionUpdateError::Rejected(rejection, release) => {
                Self::Rejected(rejection, Effect::Change, release)
            }
            SectionUpdateError::OutcomeUnknown => Self::Unknown,
        }
    }
}
fn failure_result(write: Value, audited: bool) -> rmcp::model::CallToolResult {
    let mut output = rmcp::model::CallToolResult::structured(
        json!({"write":write,"index":{"status":"not_attempted"},"audit":{"status":if audited {"recorded"} else {"unavailable"}}}),
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
    with_output(match name {
        "knowledge_create_page" => merged_create_tool(true, true),
        "knowledge_update_page" => merged_update_tool(true, true),
        _ => existing_tool(name),
    })
}

fn with_output(mut tool: rmcp::model::Tool) -> rmcp::model::Tool {
    tool.output_schema = Some(common_output());
    tool
}

/// `knowledge_create_page`: a page under `parent_page_id`, or, when rows are
/// permitted, a row in `data_source_id` with typed `properties`.
fn merged_create_tool(page: bool, rows: bool) -> rmcp::model::Tool {
    if !rows {
        return existing_tool("knowledge_create_page");
    }
    let mut tool = crate::row_write::create_tool();
    tool.name = "knowledge_create_page".into();
    let mut schema = tool.input_schema.as_ref().clone();
    let fields = schema["properties"].as_object_mut().unwrap();
    fields.insert(
        "parent_page_id".into(),
        json!({"type":"string","pattern":crate::row_write::UUID,"description":"Page UUID for a new sub-page. Give this or data_source_id."}),
    );
    fields["data_source_id"]["description"] = json!(
        "UUID of the original Notion data source for a new row. Give this or parent_page_id; properties apply only to a row."
    );
    schema["required"] = json!(["root_page_id", "title", "markdown", "idempotency_key"]);
    if !page {
        schema["properties"]
            .as_object_mut()
            .unwrap()
            .remove("parent_page_id");
        schema["required"] = json!([
            "root_page_id",
            "data_source_id",
            "title",
            "markdown",
            "properties",
            "idempotency_key"
        ]);
        schema["properties"]["data_source_id"]["description"] =
            json!("UUID of the original Notion data source.");
        tool.description = Some(
            "Create one row in a data source with typed properties and Markdown. Keep the same idempotency key for retries."
                .into(),
        );
        tool.input_schema = Arc::new(schema);
        return tool;
    }
    tool.description = Some(
        "Create one page under parent_page_id, or one row in a data source with typed properties and Markdown. Keep the same idempotency key for retries."
            .into(),
    );
    tool.input_schema = Arc::new(schema);
    tool
}

/// `knowledge_update_page`: `operation` selects `append_markdown` or
/// `patch_properties`; only the permitted operations are described.
fn merged_update_tool(append: bool, patch: bool) -> rmcp::model::Tool {
    let mut tool = crate::row_write::update_tool();
    tool.name = "knowledge_update_page".into();
    let row = tool.input_schema.as_ref().clone();
    let mut fields = serde_json::Map::new();
    for key in ["root_page_id", "page_id", "idempotency_key"] {
        fields.insert(key.into(), row["properties"][key].clone());
    }
    fields["page_id"]["description"] = json!("Page UUID to update.");
    let mut operations = Vec::new();
    if append {
        operations.push("append_markdown");
        fields.insert("markdown".into(), json!({"type":"string","minLength":1,"maxLength":200000,"description":"append_markdown: exact Markdown to add at the end."}));
        fields.insert("expected_last_edited_time".into(), json!({"type":"string","maxLength":128,"description":"append_markdown: guard; give this or expected_markdown_sha256."}));
        fields.insert(
            "expected_markdown_sha256".into(),
            json!({"type":"string","pattern":"^[a-fA-F0-9]{64}$"}),
        );
    }
    if patch {
        operations.push("patch_properties");
        fields.insert(
            "expected_row_revision".into(),
            row["properties"]["expected_row_revision"].clone(),
        );
        fields.insert("properties".into(), row["properties"]["properties"].clone());
    }
    fields.insert(
        "operation".into(),
        json!({"type":"string","enum":operations}),
    );
    tool.description = Some(
        "Update one authorized page. append_markdown adds Markdown at the end under a revision guard. patch_properties changes selected row properties under the authoritative row_revision. Reuse the same idempotency key after an uncertain response."
            .into(),
    );
    tool.input_schema = Arc::new(
        json!({"type":"object","additionalProperties":false,
            "required":["root_page_id","page_id","operation","idempotency_key"],"properties":fields})
        .as_object()
        .unwrap()
        .clone(),
    );
    tool
}

/// The capability selector a merged call's operation is governed by.
pub(crate) fn effective_operation(
    name: &str,
    arguments: &serde_json::Map<String, Value>,
) -> Option<&'static str> {
    match name {
        "knowledge_update_page" => match arguments.get("operation").and_then(Value::as_str) {
            Some("append_markdown") => Some("knowledge_append"),
            Some("patch_properties") => Some(crate::row_write::UPDATE_SELECTOR),
            _ => None,
        },
        "knowledge_create_page" if arguments.contains_key("data_source_id") => {
            Some(crate::row_write::CREATE_SELECTOR)
        }
        _ => None,
    }
}

fn existing_tool(name: &str) -> rmcp::model::Tool {
    let mut tool = crate::write_contract::tool(name).expect("implemented semantic mutation");
    tool.description = Some(match name {
        "knowledge_create_page" => "Create one page under an explicitly authorized parent with once-only idempotency, authoritative read-back and separate local refresh status.",
        _ => "Update one exact heading's body with fresh revision guards, trusted server confirmation, once-only idempotency and separate local refresh status.",
    }.into());
    let mut schema = tool.input_schema.as_ref().clone();
    let properties = schema
        .get_mut("properties")
        .unwrap()
        .as_object_mut()
        .unwrap();
    properties.get_mut("root_page_id").expect("root argument")["description"] = json!(ROOT_FIELD);
    let (destination, description) = if name == "knowledge_create_page" {
        (
            "parent_page_id",
            "Page UUID where the new page will be created.",
        )
    } else {
        ("page_id", "Page UUID to update.")
    };
    properties
        .get_mut(destination)
        .expect("destination argument")["description"] = json!(description);
    if name == "knowledge_update_section" {
        properties.remove("confirmation_id");
        properties.insert("section_anchor".into(),json!({"type":"object","additionalProperties":false,"required":["level","heading"],"properties":{"level":{"type":"integer","minimum":1,"maximum":6},"heading":{"type":"string","minLength":1,"maxLength":200}}}));
        properties.insert(
            "markdown".into(),
            json!({"type":"string","minLength":0,"maxLength":200000}),
        );
        if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
            required.retain(|v| v != "confirmation_id");
        }
    }
    tool.input_schema = Arc::new(schema);
    tool
}

fn common_output() -> Arc<serde_json::Map<String, Value>> {
    Arc::new(json!({
        "type":"object","required":["write","index","audit"],
        "properties":{
            "write":{"type":"object","required":["status"],"properties":{
                "status":{"enum":["verified","cancelled","invalid_input","not_authorized","revision_conflict","idempotency_conflict","unavailable","outcome_unknown"]},
                "page_id":{"type":"string"},"url":{"type":"string"},"last_edited_time":{"type":"string"},"current_last_edited_time":{"type":"string"},"replayed":{"type":"boolean"},
                "reason":{"enum":["root_not_configured","not_found","permission_denied","invalid_request","parent_is_database"]},
                "message":{"type":"string"}
            }},
            "index":{"type":"object","required":["status"],"properties":{
                "status":{"enum":["queued","running","completed","failed","unavailable","not_attempted"]},
                "refresh_id":{"type":"string"}
            }},
            "audit":{"type":"object","required":["status"],"properties":{"status":{"enum":["recorded","unavailable"]}}}
        }
    }).as_object().unwrap().clone())
}

/// The installed write surface is deliberately narrower than all mutation
/// schemas: file upload, archive and whole-page replacement are separate work.
pub fn is_semantic_mutation(name: &str) -> bool {
    matches!(
        name,
        "knowledge_create_page" | "knowledge_update_page" | "knowledge_update_section"
    )
}
