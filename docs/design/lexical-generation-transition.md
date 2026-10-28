# Lexical generation transitions

The offline candidate, activation, and rollback framework has been removed.
This service runs in one process; Notion is authoritative and its local
retrieval index is a disposable cache.

Use [cache reset and authoritative resync](../cache-reset-and-resync.md) for recovery.
Credentials, authorization, and external-write records remain protected.
Lexical identity checks remain required; installed expansion and retrieval
quality still require their own acceptance work (#491, #492, #493).
