# tests

Tests for the TypeScript SDK. `npm test` runs every file below except
`pdb_wire.test.ts`, one file at a time (`--test-concurrency=1`); run one file
with `node --import tsx --test tests/<file>`.

| File | What it is for |
|------|----------------|
| `arrow_ipc.test.ts` | `src/arrow_ipc.ts` codec: decodes a pyarrow-written stream (embedded hex) with every result type the server ships, checks the encoder's IPC framing / 8-byte alignment, round-trips Int64 / Float64 / Utf8 / Boolean / Binary with nulls and 10k rows, and, when `python3` + `pyarrow` are on PATH, has pyarrow read the encoder output (skipped otherwise). |
| `client.test.ts` | Actual client and helper classes imported from `src/`, with a recording pool for SQL generation and lifecycle checks, plus malformed-URL error redaction including hidden input/cause fields (`node:test`). |
| `integration.test.ts` | Integration smoke test (`node:test`). |
| `driver_surface.test.ts` | SQLSTATE decoding (`sqlstateForCode` / `sqlstateOf`, pure) and, when `PDB_NATIVE_PORT` is set, the driver-layer surface the ORM adapters use: `queryArrays` (duplicate labels, `fields` / `decl`, exact timestamps, DML counts), `QueryError.sqlstate`, `reserve()` (one session, a rolled-back COMMIT flagged, `release()` rolling back an open block) -- each checked against the table (`node:test`). |
| `vector_injection.test.ts` | SQL-injection regressions for `VectorOps` on a recording pool: hostile vector elements, `k`, nested arrays, NaN / Infinity and hostile table / column / `data`-key / option names are refused before any statement is sent; insert values and the vector and `k` are bound; string `createIndex` options (quotes, `\'`, JSON with backslashes, unicode) stay inside one `'...'` literal (`node:test`). |
| `vector_injection.live.test.ts` | Live counterpart (`PDB_NATIVE_PORT`, tables `ts_inj_vec` / `ts_inj_canary`): the payloads are stored byte for byte by `insert`, refused by `search`, read by the server as an option value by `createIndex`, and the canary table survives every case (`node:test`). |
| `ops-namespace.test.ts` | Operations-namespace helpers (`node:test`). |
| `pdb_wire.test.ts` | `PdbWireClient` (native wire) against a mocked socket. Written for Jest (`jest.mock('net')`); Jest is not a declared dev dependency, so this file does not run under `npm test` or `node --test`. |
| `unix_socket.test.ts` | Unix-socket selection: `isLocalHost` literals (no DNS), `unixSocketPath` convention with platform-native path separators, `nativeEndpoints` for `auto` / `tcp` / `unix` with and without the file, `?transport=` validation. LIVE part (`PDB_NATIVE_PORT`): `health().transport` is `unix` on 127.0.0.1 / localhost and `tcp` with `transport: 'tcp'` or another `unixSocketDir` (`node:test`). |
| `pdb_wire_ambiguous.test.ts` | Real-TCP fake server (no ParticleDB) counting requests and accepts: a batch / statement / query / transaction whose connection drops or times out after the request was sent rejects with `ConnectionError`, is received exactly once, and its connection is not reused; a refused connect and a dropped HELLO are still retried (`node:test`). |
| `native_batch_errors.live.test.ts` | Live native-wire test: a failed statement inside `batch()` / `transaction()` must reject with the server's `duplicate key` error, the pooled connection must be usable afterwards, and successful batches / transactions must still succeed. Skipped unless `PDB_NATIVE_PORT` is set (host 127.0.0.1, `--no-auth` server). Prints `NBE_PASS typescript` when every case passed. |

| `native_default.live.test.ts` | Live native-wire test of the default client: `ParticleDB` uses protocol `pdb`; plain query types; parameters through PREPARE/EXECUTE (Int64, Float64, Utf8, Boolean, Binary, null); a 10,000-row `executeBatch` (one MSG_EXECUTE_BATCH) checked by count, sum(id), sum(score), non-null counts and a spot-checked row; bad SQL / parameter-count errors as `QueryError`; a duplicate key inside an autocommit batch as `BatchExecuteError` with the other rows committed; the same inside `transaction()` rolling everything back; statement-cache eviction (MSG_CLOSE_STMT). Skipped unless `PDB_NATIVE_PORT` is set (host 127.0.0.1, `--no-auth` server). Prints `NDL_PASS typescript` when all 7 cases passed. |
| `native_regress.live.test.ts` | Live regressions: a statement error caught inside `transaction()` rejects with `TransactionRolledBackError` (COMMIT answered with `FLAG_TXN_ROLLED_BACK`) and commits nothing; a zero-parameter `executeBatch` runs once per row; a batch whose last row exceeds the frame cap is refused with `RangeError` before any frame is sent; a bigint above 2^53 in a float-promoted column is refused, not rounded; a Decimal256 (over 38 digits) result decodes exactly. Each case checks the table. Skipped unless `PDB_NATIVE_PORT` is set. |

Live tests:

```bash
PDB_NATIVE_PORT=7173 node --import tsx --test tests/native_batch_errors.live.test.ts
PDB_NATIVE_PORT=7173 node --import tsx --test tests/native_default.live.test.ts
PDB_NATIVE_PORT=7173 node --import tsx --test tests/native_regress.live.test.ts
```

The `node --test` reporter prefixes child-process stdout with `# `, so the
marker appears as `# NBE_PASS typescript` there; run the file directly
(`node --import tsx tests/native_batch_errors.live.test.ts`) to get the bare
`NBE_PASS typescript` line.

`tls.live.test.ts` checks verified native TLS, rejected credentials, pool waiter cleanup, transaction ordering, timeouts and pipeline DDL against configured live fixtures. A client-only Windows run skips these live cases and does not certify database integration.
