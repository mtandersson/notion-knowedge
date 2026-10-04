# Embedding provider contract

`core::embedding::EmbeddingProvider` is an object-safe asynchronous batch port.
Concrete local runtime implementations belong in `retrieval`; no runtime or
model dependency is introduced by this contract. The deterministic fake lives
only in core integration tests. A real model adapter and index persistence are
separate follow-up work (#39 and #37/#36).

Each instance exposes immutable `EmbeddingMetadata`: provider ID, model ID,
model version and positive vector dimension. Version means the identity of the
weights and preprocessing that determine the vector space, not merely a package
release. Persist this versioned record alongside the index even when empty.
Deserialization rejects unsupported schemas and invalid identities. Before
inserting or querying, compare persisted metadata with the selected provider via
`ensure_compatible`; a mismatch requires an explicit index rebuild, including
same-dimension changes. This contract defines the record, not a storage engine.

Application callers use `embedding::embed_batch` to validate exact cardinality,
dimension and finite components before indexing. Output position corresponds to
input position, including duplicates; adapters must preserve order (the boundary
cannot infer reordered vectors). Empty batches avoid runtime execution. Inputs
are owned strings borrowed for the future, with no truncation or normalization
performed by core. Model-specific input limits belong to the adapter. Failure
is whole-batch; partial results are not committed.

`Unavailable` and `ResourceExhausted` are retryable; other typed failures are
permanent. Errors contain no text, credentials or raw runtime messages. Retry
limits, delays and cancellation belong to orchestration, not the port. An
adapter must honor cancellation by dropping its future and must document any
non-cancellable runtime work. Tests run with `cargo test -p notion-knowledge-core
--test embedding` and exercise the application boundary with a deterministic
fake, malformed output, retry classification and persisted compatibility.
