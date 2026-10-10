//! SQLite derived alias lookup. Distinct canonical page IDs, never guessed
//! from ambiguous names. Independent of vector/chunk storage and Notion APIs.

use notion_knowledge_core::{
    aliases::{PageAlias, PageAliasStore, normalized},
    sync_state::{SyncStateError, validate_identifier},
};
use rusqlite::{TransactionBehavior, params};

use crate::sync_state::SqliteSyncStateStore;

impl PageAliasStore for SqliteSyncStateStore {
    fn replace_page_aliases(
        &self,
        workspace_id: &str,
        root_page_id: &str,
        page_id: &str,
        aliases: &[PageAlias],
    ) -> Result<(), SyncStateError> {
        for id in [workspace_id, root_page_id, page_id] {
            validate_identifier(id)?;
        }
        // Validate everything before removing old entries. No partial
        // replacement on invalid records, prepare/insert errors or disk failure.
        if aliases.len() > 128 {
            return Err(SyncStateError::InvalidInput);
        }
        let mut connection = self.lock_connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_failure)?;
        transaction
            .execute(
                "DELETE FROM graph_aliases
                 WHERE workspace_id = ?1 AND root_page_id = ?2 AND page_id = ?3",
                params![workspace_id, root_page_id, page_id],
            )
            .map_err(sqlite_failure)?;
        {
            let mut statement = transaction
                .prepare(
                    "INSERT INTO graph_aliases
                       (workspace_id, root_page_id, page_id, alias_key, display_name, provenance)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT DO NOTHING",
                )
                .map_err(sqlite_failure)?;
            for alias in aliases {
                statement
                    .execute(params![
                        workspace_id,
                        root_page_id,
                        page_id,
                        alias.key(),
                        alias.display(),
                        alias.provenance()
                    ])
                    .map_err(sqlite_failure)?;
            }
        }
        transaction.commit().map_err(sqlite_failure)
    }

    fn lookup_alias(
        &self,
        workspace_id: &str,
        root_page_id: &str,
        alias: &str,
    ) -> Result<Vec<String>, SyncStateError> {
        validate_identifier(workspace_id)?;
        validate_identifier(root_page_id)?;
        let key = normalized(alias)?;
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT DISTINCT page_id FROM graph_aliases
                 WHERE workspace_id = ?1 AND root_page_id = ?2 AND alias_key = ?3
                 ORDER BY page_id",
            )
            .map_err(sqlite_failure)?;
        let rows = statement
            .query_map(params![workspace_id, root_page_id, key], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sqlite_failure)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sqlite_failure)
    }
}

fn sqlite_failure(_: rusqlite::Error) -> SyncStateError {
    SyncStateError::Unavailable
}
