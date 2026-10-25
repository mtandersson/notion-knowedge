use super::*;
use crate::{
    commit::{CommitError, IndexCommitCoordinator, PageAction, PageOperation},
    rollback_runtime::probe,
};
use notion_knowledge_core::{
    embedding::{EmbeddingFuture, EmbeddingProvider},
    reconciliation::ReconciliationJournal,
    sync_state::PageSyncState,
};
fn private() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn bytes(root: &Path) -> Vec<(PathBuf, String)> {
    fn walk(p: &Path, out: &mut Vec<(PathBuf, String)>) {
        for entry in fs::read_dir(p).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.push((
                    p.clone(),
                    format!("{:x}", Sha256::digest(fs::read(p).unwrap())),
                ));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, &mut out);
    out.sort();
    out
}
struct Bits(EmbeddingMetadata);
impl EmbeddingProvider for Bits {
    fn metadata(&self) -> &EmbeddingMetadata {
        &self.0
    }
    fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            Ok(inputs
                .iter()
                .map(|_| {
                    vec![
                        f32::from_bits(0x80000000),
                        f32::from_bits(1),
                        f32::from_bits(0x7f7fffff),
                    ]
                })
                .collect())
        })
    }
}
#[test]
fn owned_transfer_preserves_latest_native_rows_raw_strings_and_exact_vector_bits() {
    for bound in [false, true] {
        let parent = private();
        let root = parent.path().join("source");
        let dictionary = bound.then(|| {
            crate::lexical_dictionary::tests::dictionary(&[("cinnamon roll", "kanelbulle")])
        });
        probe::create(&root, dictionary.as_ref()).unwrap();
        let (binding, embedding) = probe::fixture_configuration(!bound).unwrap();
        let c = IndexCommitCoordinator::open_existing(
            root.join("index"),
            root.join("state.sqlite"),
            binding,
        )
        .unwrap();
        let limits = TransferLimits::default();
        let path = parent.path().join("rows");
        let before = bytes(&root);
        let original = c
            .with_owned_offline_inspection(10, |owner| {
                export(owner, &path, &embedding, limits).map_err(|_| CommitError::BindingMismatch)
            })
            .unwrap();
        assert_eq!(original.metadata.source.lexical.is_some(), bound);
        assert_eq!(bytes(&root), before);
        let plan = dictionary
            .as_ref()
            .map(crate::chunks::lexical_binding::LexicalPlanIdentity::verified)
            .transpose()
            .unwrap();
        let table = if let Some(plan) = plan {
            GuardedChunkTable::bind_with_lexical_plan(
                root.join("index"),
                c.binding().table(),
                embedding.clone(),
                c.clone(),
                &plan,
            )
            .unwrap()
        } else {
            GuardedChunkTable::bind(
                root.join("index"),
                c.binding().table(),
                embedding.clone(),
                c.clone(),
            )
            .unwrap()
        };
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut chunk = rt
            .block_on(table.page_chunks("NK-PROBE-PAGE-123"))
            .unwrap()
            .pop()
            .unwrap();
        chunk.text = "new café\nline \\".into();
        chunk.content_hash = "new-content".into();
        chunk.metadata.last_edited_time = "new-revision".into();
        let operation = PageOperation::new(
            "transfer-new-row",
            "new-revision",
            PageAction::Refresh,
            PageSyncState::present(
                chunk.metadata.page_id.clone(),
                chunk.content_hash.clone(),
                Some("new-revision".into()),
            )
            .unwrap(),
        )
        .unwrap();
        let lease = c.state().acquire_lease(20, 100).unwrap();
        let work = rt
            .block_on(table.prepare_page(&Bits(embedding.clone()), operation, &[chunk]))
            .unwrap();
        rt.block_on(table.commit_page(work, lease.clone(), Arc::new(|| 20), || async { Ok(()) }))
            .unwrap();
        c.state().release_lease(&lease, 20).unwrap();
        drop(rt);
        let latest = bytes(&root);
        assert_ne!(latest, before);
        let fresh_path = parent.path().join("latest");
        let fresh = c
            .with_owned_offline_inspection(21, |owner| {
                assert!(export(owner, &path, &embedding, limits).is_err());
                assert!(export(owner, &root.join("index/forbidden"), &embedding, limits).is_err());
                let small = parent.path().join("bounded");
                assert!(
                    export(
                        owner,
                        &small,
                        &embedding,
                        TransferLimits { bytes: 8, ..limits }
                    )
                    .is_err()
                );
                assert!(small.exists());
                assert!(fs::metadata(small).unwrap().len() <= 8);
                export(owner, &fresh_path, &embedding, limits)
                    .map_err(|_| CommitError::BindingMismatch)
            })
            .unwrap();
        assert_ne!(
            original.metadata.source.snapshot_sha256,
            fresh.metadata.source.snapshot_sha256
        );
        assert_ne!(original.metadata.operational, fresh.metadata.operational);
        let mut observed = 0;
        read(&fresh_path, &fresh, limits, |b| {
            let row = row(b, 0)?;
            assert_eq!(row.vector_bits, vec![0x80000000, 1, 0x7f7fffff]);
            assert_eq!(row.strings[13].as_deref(), Some("new café\nline \\"));
            observed += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(observed, 1);
        streamed_consumption(parent.path(), &fresh_path, &fresh, limits);
        if bound {
            opaque_native_description(&fresh_path, &fresh, limits);
        }
        assert_eq!(bytes(&root), latest);
        refusals(&fresh_path, &fresh, limits);
        assert_eq!(bytes(&root), latest);
    }
}
fn rewrite(path: &Path, expected: &TransferRecord, data: &[u8]) -> TransferRecord {
    fs::write(path, data).unwrap();
    TransferRecord {
        bytes: data.len() as u64,
        sha256: format!("{:x}", Sha256::digest(data)),
        ..expected.clone()
    }
}
fn refusals(path: &Path, record: &TransferRecord, limits: TransferLimits) {
    let original = fs::read(path).unwrap();
    let noop = |_: &RecordBatch| Ok(());
    assert!(
        read(
            path,
            record,
            TransferLimits {
                bytes: record.bytes - 1,
                ..limits
            },
            noop
        )
        .is_err()
    );
    assert!(
        read(
            path,
            record,
            TransferLimits {
                native: CandidateLimits {
                    identifier_bytes: 1,
                    ..limits.native
                },
                ..limits
            },
            noop
        )
        .is_err()
    );
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(read(path, record, limits, noop).is_err());
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = path.with_extension("alias");
    std::os::unix::fs::symlink(path, &alias).unwrap();
    assert!(read(&alias, record, limits, noop).is_err());
    fs::remove_file(&alias).unwrap();
    fs::hard_link(path, &alias).unwrap();
    assert!(read(path, record, limits, noop).is_err());
    fs::remove_file(&alias).unwrap();
    for data in [
        &original[..original.len() - 1],
        &[original.clone(), vec![0]].concat(),
    ] {
        let changed = rewrite(path, record, data);
        assert!(read(path, &changed, limits, noop).is_err());
    }
    let mut unsupported = record.clone();
    unsupported.metadata.version = 99;
    let header_len = u32::from_le_bytes(original[8..12].try_into().unwrap()) as usize;
    let mut data = MAGIC.to_vec();
    frame(&mut data, &unsupported.metadata, HEADER_BYTES).unwrap();
    data.extend_from_slice(&original[12 + header_len..]);
    let changed = rewrite(path, &unsupported, &data);
    assert!(read(path, &changed, limits, noop).is_err());
    for variant in ["schema", "source-version", "ownership", "sql-bound"] {
        let mut bad_record = record.clone();
        match variant {
            "schema" => bad_record.metadata.canonical_schema = "unsupported".into(),
            "source-version" => bad_record.metadata.source.source_version = 0,
            "ownership" => bad_record.metadata.ownership_sha256 = "invalid".into(),
            _ => bad_record.metadata.operational.bytes = limits.sql_bytes + 1,
        }
        let mut data = MAGIC.to_vec();
        frame(&mut data, &bad_record.metadata, HEADER_BYTES).unwrap();
        data.extend_from_slice(&original[12 + header_len..]);
        let bad_record = rewrite(path, &bad_record, &data);
        assert!(read(path, &bad_record, limits, noop).is_err());
    }
    let mut data = original.clone();
    data[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    let changed = rewrite(path, record, &data);
    assert!(read(path, &changed, limits, noop).is_err());
    let start = 12 + header_len;
    let mut input = &original[start..];
    let mut changed_row: Row = read_frame(&mut input, limits.frame()).unwrap();
    changed_row.vector_bits[0] = 0;
    let mut data = original[..start].to_vec();
    frame(&mut data, &changed_row, limits.frame()).unwrap();
    let changed = rewrite(path, record, &data);
    assert!(read(path, &changed, limits, noop).is_err());
    fs::write(path, &original).unwrap();
    let old = path.with_extension("old");
    assert!(
        read(path, record, limits, |_| {
            fs::rename(path, &old)?;
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(path)?;
            f.write_all(&original)?;
            Ok(())
        })
        .is_err()
    );
    fs::remove_file(path).unwrap();
    fs::rename(old, path).unwrap();
    assert_eq!(fs::read(path).unwrap(), original);
}

fn streamed_consumption(
    parent: &Path,
    path: &Path,
    record: &TransferRecord,
    limits: TransferLimits,
) {
    let mut callback = Vec::new();
    read(path, record, limits, |b| {
        callback.push(json(&row(b, 0)?, limits.frame())?);
        Ok(())
    })
    .unwrap();
    let streamed = Reader::open(path, record, limits)
        .unwrap()
        .map(|b| json(&row(&b.unwrap(), 0).unwrap(), limits.frame()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(streamed, callback);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let database_path = parent.join("stream-ingestion");
    let db = rt
        .block_on(lancedb::connect(&local_database_uri(&database_path).unwrap()).execute())
        .unwrap();
    let reader = Reader::open(path, record, limits).unwrap();
    let table = rt
        .block_on(
            db.create_table(
                "complete",
                Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>,
            )
            .execute(),
        )
        .unwrap();
    assert_eq!(
        rt.block_on(table.count_rows(None)).unwrap() as u64,
        record.metadata.source.rows
    );
    let batches = rt.block_on(async {
        table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    });
    let stored = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |i| json(&row(b, i).unwrap(), limits.frame()).unwrap())
        })
        .collect::<Vec<_>>();
    assert_eq!(stored, callback);
    // Correct complete-file SHA does not excuse a bad trailing frame/digest.
    // The Arrow consumer must receive the final error, not a successful EOF.
    let original = fs::read(path).unwrap();
    let bad = rewrite(path, record, &[original.clone(), vec![0]].concat());
    let mut reader = Reader::open(path, &bad, limits).unwrap();
    assert!(reader.next().unwrap().is_ok());
    assert!(reader.next().unwrap().is_err());
    assert!(reader.next().is_none());
    let reader = Reader::open(path, &bad, limits).unwrap();
    assert!(
        rt.block_on(
            db.create_table(
                "incomplete",
                Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>
            )
            .execute()
        )
        .is_err()
    );
    fs::write(path, original).unwrap();
}
fn opaque_native_description(path: &Path, record: &TransferRecord, limits: TransferLimits) {
    let original = fs::read(path).unwrap();
    let header_len = u32::from_le_bytes(original[8..12].try_into().unwrap()) as usize;
    let original_binding =
        serde_json::to_value(record.metadata.source.lexical.as_ref().unwrap()).unwrap();
    for case in [
        "opaque",
        "version",
        "embedding",
        "policy",
        "physical",
        "embedding-contract",
        "canonical",
    ] {
        let mut binding = original_binding.clone();
        match case {
            "opaque" => {
                binding["storage"]["backend"] = "another-native-backend".into();
                binding["storage"]["schema"] = "another-native-layout".into();
                binding["storage"]["profile"]["language"] = "opaque-tokenizer-language".into();
                binding["plan"]["executor_sha256"] = "e".repeat(64).into();
            }
            "version" => binding["version"] = 99.into(),
            "embedding" => binding["embedding"]["model_id"] = "other-model".into(),
            "policy" => binding["scope"] = "f".repeat(64).into(),
            "physical" => binding["generation"] = "other-generation".into(),
            "embedding-contract" => {
                binding["plan"]["embedding_text_contract"] = "unsupported".into()
            }
            _ => binding["storage"]["canonical_schema"] = "unsupported".into(),
        }
        let mut expected = record.clone();
        expected.metadata.source.lexical = Some(serde_json::from_value(binding).unwrap());
        let mut data = MAGIC.to_vec();
        frame(&mut data, &expected.metadata, HEADER_BYTES).unwrap();
        data.extend_from_slice(&original[12 + header_len..]);
        let expected = rewrite(path, &expected, &data);
        let mut count = 0;
        let result = read(path, &expected, limits, |_| {
            count += 1;
            Ok(())
        });
        assert_eq!(result.is_ok(), case == "opaque", "{case}");
        assert_eq!(count, if case == "opaque" { 1 } else { 0 });
        let result = Reader::open(path, &expected, limits)
            .and_then(|reader| reader.collect::<Result<Vec<_>, _>>().map_err(|_| bad()));
        assert_eq!(result.is_ok(), case == "opaque", "{case}");
    }
    // This exercises an untrusted descriptive envelope, not a verified source
    // on that backend. Both source ownership and runtime proof remain separate.
    fs::write(path, original).unwrap();
}
