# src

Source of the `particledb` npm package (compiled by `tsc` to `dist/`).

| File | What it is for |
|------|----------------|
| `index.ts` | Package entry point: re-exports the public classes, errors and types. |
| `client.ts` | `ParticleDB` (the main client) and `Transaction`; `queryArrays` / `reserve` delegate to the native client (refused on the PG backend). Runs on a `Backend`: `NativeBackend` over `PdbWireClient` (the DEFAULT, port 7173) or `PgBackend` over a `pg` Pool, selected only by `protocol: 'pg'` or a `postgresql://` connection string. `loadPg` requires `pg` lazily there, so the package builds and runs without `pg` installed. |
| `pdb_wire.ts` | `PdbWireClient`, the PDB native wire client: framing, HELLO / AUTH, connection pool, `query` / `execute` / `batch` / `transaction`, native prepared statements (`Connection.ensurePrepared` = MSG_PREPARE with a per-connection LRU closed by MSG_CLOSE_STMT, `Connection.run` = MSG_EXECUTE) and `executeBatch` (`Connection.runBatch` = MSG_EXECUTE_BATCH, `decodeExecuteBatchResult`, `BatchExecuteError`). Error classes `PdbError`, `ConnectionError`, `QueryError` (with `sqlstate`, decoded by `sqlstateOf` / `sqlstateForCode`: the frame code, the 6xxxx band and the engine's `[XXXXX]` marker, the same table as the Python SDK), `DiskFullError`. Driver layer: `queryArrays` (`frameToRaw`: positional rows, `fields`, `rowCount`, `rolledBack` from `FLAG_TXN_ROLLED_BACK`) on the client and `TransactionContext`, and `reserve()` -> `ReservedConnection` (one pooled connection, statements in issue order, tracks BEGIN / COMMIT / ROLLBACK text because protocol 1 reports no transaction status, `release()` rolls back an open block, `destroy()` closes the socket). Transport: `nativeEndpoints` (exported) decides the order - `'auto'` gives a local host (`isLocalHost`: `localhost`, `127.x`, `::1`, no DNS) the socket `unixSocketPath(port, unixSocketDir)` first when the file exists, then TCP (a failed socket falls back); `'tcp'` opts out; `'unix'` forces it. `health().transport` reports the kind. |
| `arrow_ipc.ts` | Dependency-free Arrow IPC stream codec: `decodeArrowStream` (results; hand-rolled flatbuffer reader; `shape: 'array'` for positional rows, `fields` with each column's Arrow type token and custom metadata such as `pdb:decl`, `DecodeOptions.timestamps: 'string'` for exact `YYYY-MM-DD HH:MM:SS[.f]` text) and `encodeArrowStream` (parameter batches: Int64, Float64, Utf8, Boolean, Binary with nulls; back-to-front flatbuffer builder `FbBuilder`), plus `inferParamType` / `normalizeParam` (JS value → Arrow parameter type, pg-compatible text for Date / array / object). |
| `types.ts` | Config and result interfaces (`ParticleDBConfig`, `ConnectionOptions`, `QueryResult`, ...) and `Queryable`, the `query(sql, params)` surface the helper modules run on. |
| `vector.ts`, `kv.ts`, `rag.ts`, `ai.ts`, `ops.ts` | `VectorOps`, `KVOps`, `RAGOps`, `AIOps`, `ExtendedOps`: SQL helpers over a `Queryable` (the client's native backend by default). |
| `sql.ts` | The SDK's one set of SQL text encoders: `quoteIdent`, `escapeLiteral` (standard-conforming `'...'` body only) and `assertIdentifier` (unquoted identifier, optionally `schema.table`). Used by `ops.ts`, `rag.ts` and `vector.ts`. Values are bound parameters wherever the statement allows one. |
| `streaming.ts` | `StreamingClient`: WebSocket / SSE CDC and row streaming over the HTTP port. |
| `grpc.ts` | `GrpcClient`: gRPC client (optional `@grpc/grpc-js` peer). |

Constraints: `arrow_ipc.ts` must stay dependency-free (it is what lets the
package ship without `pg` or `apache-arrow`); the server caps one frame at
16 MiB (`MAX_PAYLOAD_SIZE` in `pdb_wire.ts`, mirrored from the server), which
`Connection.runBatch` splits around. `runBatch` encodes EVERY frame before
sending the first (outside a transaction each sent frame commits, so a bad
value or an oversized row must be refused while nothing is committed), and
runs a zero-parameter statement once per row with plain QUERY (an
EXECUTE_BATCH with no parameter columns carries no row count and runs once).
`transaction()` reads `FLAG_TXN_ROLLED_BACK` on the COMMIT reply: a COMMIT
that rolled back a failed block rejects with `TransactionRolledBackError`.
`encodeArrowStream` refuses a bigint above 2^53 in a Float64 column rather
than round it.

`VectorOps` binds the query vector and `k` (`$1`, `$2`) and the insert values,
and runs `assertIdentifier` on every name it puts into SQL text. Vectors must be
finite numbers and `k` a positive safe integer, checked at runtime. `createIndex` options are the
only literal it builds (DDL takes no parameters), through `escapeLiteral`.
`search`'s `filter` is caller-written SQL by contract.

URL-constructor failures and unsupported schemes return static `PdbError`
messages. Never retain the constructor's input or cause on that error.
