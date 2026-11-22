//! Whole-page canonical source snapshots, independent of the chunk/vector
//! publication. Same directory binding and owned writer as the search index.
use crate::commit::{CommitError, IndexCommitCoordinator};
use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lancedb::{
    Table,
    query::{ExecutableQuery, QueryBase, Select},
};
use notion_knowledge_core::{
    backend::{Page, PageContent, PageId},
    fingerprint::content_hash,
    indexed::IndexedDocument,
    inventory::InventoryParent,
    reconciliation::{FailureClass, Lease},
    root_scope::canonical_id,
    source::{CachedPage, CachedPageFuture, FreshPageCache, FreshPageStamp},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;

const TABLE: &str = "page_documents";
const VERSION: (&str, &str) = ("notion_knowledge.page_documents.version", "1");

#[derive(Clone)]
pub struct PageDocuments(Arc<IndexCommitCoordinator>);

struct StoredDocument {
    document: IndexedDocument,
    stamp: FreshPageStamp,
}

impl PageDocuments {
    pub fn new(coordinator: Arc<IndexCommitCoordinator>) -> Self {
        Self(coordinator)
    }

    /// Publish one trusted source snapshot before any model work. Repeated
    /// clean reads of the same document reuse the already committed row.
    pub async fn publish(
        &self,
        document: IndexedDocument,
        parent: &InventoryParent,
        watermark: i64,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), CommitError> {
        if !canonical_id(&document.metadata.page_id)
            || !self
                .0
                .binding()
                .roots()
                .contains(&document.metadata.source.root_page_id)
            || self.0.binding().workspace() != document.metadata.source.workspace_id
            || document.metadata.last_edited_time.is_empty()
            || document.metadata.url.is_empty()
            || watermark < 0
        {
            return Err(CommitError::InvalidInput);
        }
        let parent = serde_json::to_string(parent).map_err(|_| CommitError::InvalidInput)?;
        let store = self.clone();
        self.0
            .maintain(lease, clock, move |context| async move {
                let page = &document.metadata.page_id;
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                let table = store.open(true).await?.ok_or(FailureClass::Index)?;
                if let Some(old) = store.load(&table, page).await?
                    && old.document == document
                    && old.stamp.parent == parent
                    && store.current_under_guard(&old.stamp)
                {
                    return Ok(());
                }

                let payload = serde_json::to_string(&document).map_err(|_| FailureClass::Index)?;
                let binding = store.binding_identity()?;
                let publication = publication_id(&binding, &parent, &payload, watermark);
                let schema = schema();
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(StringArray::from(vec![page.as_str()])),
                        Arc::new(StringArray::from(vec![binding.as_str()])),
                        Arc::new(StringArray::from(vec![parent.as_str()])),
                        Arc::new(StringArray::from(vec![payload.as_str()])),
                        Arc::new(Int64Array::from(vec![watermark])),
                    ],
                )
                .map_err(|_| FailureClass::Index)?;
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                let mut merge = table.merge_insert(&["page_id"]);
                merge
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                merge
                    .execute(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
                    .await
                    .map_err(|_| FailureClass::Index)?;
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                store
                    .0
                    .state()
                    .set_source_document_publication(
                        context.binding().workspace(),
                        context.binding().scope(),
                        page,
                        Some(&publication),
                    )
                    .map_err(|_| FailureClass::Index)
            })
            .await
    }

    pub async fn remove(
        &self,
        page: PageId,
        lease: Lease,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<(), CommitError> {
        if !canonical_id(&page.0) {
            return Err(CommitError::InvalidInput);
        }
        let store = self.clone();
        self.0
            .maintain(lease, clock, move |context| async move {
                if let Some(table) = store.open(false).await? {
                    context.revalidate().map_err(|_| FailureClass::Conflict)?;
                    table
                        .delete(&format!("page_id = '{}'", page.0))
                        .await
                        .map_err(|_| FailureClass::Index)?;
                }
                store
                    .0
                    .state()
                    .set_source_document_publication(
                        context.binding().workspace(),
                        context.binding().scope(),
                        &page.0,
                        None,
                    )
                    .map_err(|_| FailureClass::Index)
            })
            .await
    }

    /// Includes source-only pages that have no published embedding checkpoint.
    pub async fn page_ids(&self) -> Result<Vec<String>, FailureClass> {
        let Some(table) = self.open(false).await? else {
            return Ok(Vec::new());
        };
        let mut stream = table
            .query()
            .select(Select::Columns(vec!["page_id".into()]))
            .execute()
            .await
            .map_err(|_| FailureClass::Index)?;
        let mut result = Vec::new();
        while let Some(batch) = stream.try_next().await.map_err(|_| FailureClass::Index)? {
            let ids = string_column(&batch, "page_id")?;
            for id in ids.iter() {
                let id = id
                    .filter(|id| canonical_id(id))
                    .ok_or(FailureClass::Index)?;
                result.push(id.to_owned());
            }
        }
        Ok(result)
    }

    async fn open(&self, create: bool) -> Result<Option<Table>, FailureClass> {
        let uri = self
            .0
            .index_directory()
            .to_str()
            .ok_or(FailureClass::Index)?;
        let db = lancedb::connect(uri)
            .execute()
            .await
            .map_err(|_| FailureClass::Index)?;
        let table = match db.open_table(TABLE).execute().await {
            Ok(table) => table,
            Err(lancedb::Error::TableNotFound { .. }) if !create => return Ok(None),
            Err(lancedb::Error::TableNotFound { .. }) => db
                .create_empty_table(TABLE, schema())
                .execute()
                .await
                .map_err(|_| FailureClass::Index)?,
            Err(_) => return Err(FailureClass::Index),
        };
        if table.schema().await.map_err(|_| FailureClass::Index)? != schema() {
            return Err(FailureClass::Index);
        }
        Ok(Some(table))
    }

    fn binding_identity(&self) -> Result<String, FailureClass> {
        serde_json::to_string(self.0.binding()).map_err(|_| FailureClass::Index)
    }

    async fn load(
        &self,
        table: &Table,
        page: &str,
    ) -> Result<Option<StoredDocument>, FailureClass> {
        if !canonical_id(page) {
            return Err(FailureClass::Index);
        }
        let mut stream = table
            .query()
            .only_if(format!("page_id = '{page}'"))
            .limit(2)
            .execute()
            .await
            .map_err(|_| FailureClass::Index)?;
        let mut matching = None;
        while let Some(batch) = stream.try_next().await.map_err(|_| FailureClass::Index)? {
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.num_rows() != 1 || matching.is_some() {
                return Err(FailureClass::Index);
            }
            matching = Some(batch);
        }
        let Some(batch) = matching else {
            return Ok(None);
        };
        let get = |name| -> Result<&str, FailureClass> {
            string_column(&batch, name)?
                .iter()
                .next()
                .flatten()
                .ok_or(FailureClass::Index)
        };
        if get("binding")? != self.binding_identity()? {
            return Ok(None);
        }
        let document: IndexedDocument =
            serde_json::from_str(get("document")?).map_err(|_| FailureClass::Index)?;
        if document.metadata.page_id != page
            || document.metadata.source.workspace_id != self.0.binding().workspace()
            || !self
                .0
                .binding()
                .roots()
                .contains(&document.metadata.source.root_page_id)
            || document.content_hash != content_hash(&document.text)
            || document.metadata.url.is_empty()
        {
            return Err(FailureClass::Index);
        }
        let watermark = batch
            .column_by_name("watermark")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .and_then(|array| array.iter().next().flatten())
            .filter(|&value| value >= 0)
            .ok_or(FailureClass::Index)?;
        let stamp = FreshPageStamp {
            page_id: PageId(page.to_owned()),
            root_page_id: document.metadata.source.root_page_id.clone(),
            revision: document.metadata.last_edited_time.clone(),
            parent: get("parent")?.to_owned(),
            watermark,
            publication: publication_id(
                get("binding")?,
                get("parent")?,
                get("document")?,
                watermark,
            ),
        };
        Ok(Some(StoredDocument { document, stamp }))
    }

    fn current_under_guard(&self, stamp: &FreshPageStamp) -> bool {
        self.0
            .state()
            .source_document_is_current(
                self.0.binding().workspace(),
                self.0.binding().scope(),
                &stamp.page_id.0,
                &stamp.root_page_id,
                &stamp.parent,
                &stamp.revision,
                stamp.watermark,
                &stamp.publication,
            )
            .unwrap_or(false)
    }
}

impl FreshPageCache for PageDocuments {
    fn lookup<'a>(&'a self, page: &'a PageId) -> CachedPageFuture<'a> {
        Box::pin(async move {
            let _guard = self.0.document_read_guard().ok()?;
            let table = self.open(false).await.ok().flatten()?;
            let row = self.load(&table, &page.0).await.ok().flatten()?;
            if self.0.identities().is_err() || !self.current_under_guard(&row.stamp) {
                return None;
            }
            let metadata = row.document.metadata;
            Some(CachedPage {
                stamp: row.stamp,
                content: PageContent {
                    page: Page {
                        id: PageId(metadata.page_id),
                        title: metadata.title,
                        url: metadata.url,
                        last_edited_time: metadata.last_edited_time,
                        archived: false,
                        properties: metadata.properties,
                        relation_names: Default::default(),
                    },
                    markdown: row.document.text,
                },
            })
        })
    }

    fn is_current(&self, stamp: &FreshPageStamp) -> bool {
        let Ok(_guard) = self.0.document_read_guard() else {
            return false;
        };
        self.current_under_guard(stamp)
    }
}

fn publication_id(binding: &str, parent: &str, document: &str, watermark: i64) -> String {
    let mut digest = Sha256::new();
    for field in [binding, parent, document] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    digest.update(watermark.to_be_bytes());
    format!("{:x}", digest.finalize())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("page_id", DataType::Utf8, false),
            Field::new("binding", DataType::Utf8, false),
            Field::new("parent", DataType::Utf8, false),
            Field::new("document", DataType::Utf8, false),
            Field::new("watermark", DataType::Int64, false),
        ],
        [(VERSION.0.to_owned(), VERSION.1.to_owned())].into(),
    ))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, FailureClass> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref())
        .ok_or(FailureClass::Index)
}
