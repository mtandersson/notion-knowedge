//! Explicit typed values accepted by database row mutations.
use std::{collections::BTreeMap, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};

use crate::{
    create::CreateError,
    idempotency::WriteIdempotencyStore,
    lifecycle::LifecycleScope,
    write_refresh::{MutationRefreshQueue, WriteCompletion},
};

pub const MAX_PROPERTIES: usize = 24;
pub const MAX_RELATIONS: usize = 10;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Date {
    pub start: String,
    pub end: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PropertyValue {
    Date(Option<Date>),
    Status(String),
    Select(Option<String>),
    Number(Option<f64>),
    Checkbox(bool),
    Url(Option<String>),
    RichText(String),
    Relation(Vec<String>),
}

pub type PropertyPatch = BTreeMap<String, PropertyValue>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRowRequest {
    pub root_page_id: String,
    pub data_source_id: String,
    pub title: String,
    pub markdown: String,
    pub properties: PropertyPatch,
    pub idempotency_key: String,
}

pub trait RowCreate: Send + Sync {
    fn create_row<'a>(
        &'a self,
        request: CreateRowRequest,
        scope: &'a LifecycleScope,
        ledger: &'a dyn WriteIdempotencyStore,
        queue: &'a dyn MutationRefreshQueue,
    ) -> Pin<Box<dyn Future<Output = Result<WriteCompletion, CreateError>> + Send + 'a>>;
}
