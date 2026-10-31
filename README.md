# @particledb/sdk

TypeScript and Node.js client for ParticleDB over the native wire.

## Install

Not yet published at 0.3.0 (npm has 0.2.1). Install from source:

```sh
git clone https://github.com/particledb-ai/particledb-typescript
cd particledb-typescript
npm install && npm run build
cd /path/to/your-app
npm install /path/to/particledb-typescript
```

Once 0.3.0 is published:

```sh
npm install @particledb/sdk
```

TypeScript projects also need `typescript` and `@types/node` as dev dependencies.

## Quick start

Start a local server on port 7173:

```sh
curl -fsSL https://get.particledb.ai | bash
particledb start --data-dir ./pdb-dev --no-auth \
  --concurrency-control optimistic --durability groupsync --kv-durability async
```

Save as `quickstart.mjs` and run `node quickstart.mjs`:

```typescript
import { ParticleDB } from '@particledb/sdk';

const db = await ParticleDB.connect();
try {
  await db.execute('CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)');
  await db.execute('INSERT INTO items VALUES ($1, $2)', [1, 'first']);
  const { rows } = await db.query('SELECT id, name FROM items');
  console.log(rows);
} finally {
  await db.close();
}
```

The package is built as CommonJS. `import` works from ES modules. From CommonJS, use `require('@particledb/sdk')` and run the code inside an async function.

## Connect

`ParticleDB.connect()` opens a pool and runs `SELECT 1`. It takes an options object or a URL.

```typescript
import { readFileSync } from 'node:fs';

const db = await ParticleDB.connect({
  host: 'db.example.com',
  user: 'app',
  password: process.env.PARTICLEDB_PASSWORD,
  ssl: { ca: readFileSync('ca.pem') },
  poolSize: 4,
  timeoutMs: 10_000,
});
```

| Name | Default | Meaning |
|------|---------|---------|
| `protocol` | `'pdb'` | `'pdb'` is the native wire. `'pg'` uses the PostgreSQL wire and needs the optional `pg` package. |
| `host` | `'localhost'` | Server host. |
| `port` | `7173` | Server port. `5432` when `protocol` is `'pg'`. |
| `user` | none (`'particledb'` for `'pg'`) | Login role. |
| `password` | none | Password. |
| `database` | `'particledb'` | PostgreSQL wire only. The native wire has no database selection. |
| `poolSize` | `10` | Maximum pooled connections. |
| `ssl` | off | `true` uses system roots. An object takes Node TLS options (`ca`, `cert`, `key`). Certificate and hostname checks are on. |
| `timeoutMs` | `30000` | Native wire: per-request timeout, including the wait for a pooled connection. |
| `transport` | `'auto'` | `'auto'` uses the Unix socket on localhost when present, else TCP. `'unix'` or `'tcp'` forces one. |
| `unixSocketDir` | `'/tmp'` | The server's `--unix-socket-dir`. |

URL form: `pdb://[user[:password]@]host[:port]` (or `particledb://`). Query options: `ssl=true|false`, `transport=auto|tcp|unix`, `unixSocketDir`. Unknown options are rejected. `postgresql://` URLs select the PostgreSQL wire.

```typescript
const db = await ParticleDB.connect('pdb://app:secret@localhost:7173?ssl=false');
```

TLS refuses plaintext fallback. `transport: 'unix'` cannot be combined with TLS.

A server started without `--no-auth` requires a password. Create the login role at startup:

```sh
particledb start --data-dir ./pdb-dev \
  --auth-method scram-sha-256 --pg-username app --pg-password-file ./pdb-password
```

The password file holds only the password. Use TLS for remote access.

## Usage

### Query

```typescript
const { rows, columns, rowCount } = await db.query('SELECT id, name FROM items');
const item = await db.queryOne<{ id: number; name: string }>(
  'SELECT id, name FROM items WHERE id = $1', [1],
);
const changed = await db.execute('DELETE FROM items WHERE id = $1', [1]);
```

`queryOne` returns the first row or `null`. `execute` returns the affected row count. A type argument describes your schema. It does not validate the data.

### Parameters

Parameters use `$1`, `$2`, ... and are bound separately from the SQL.

| Server type | JavaScript value |
|-------------|------------------|
| Integers within the safe range | `number` |
| Integers beyond the safe range | `bigint` |
| Exact decimals | `string` |
| Binary | `Buffer` |
| Timestamps | `Date` |

### Batches

```typescript
const result = await db.executeBatch(
  'INSERT INTO items (id, name) VALUES ($1, $2)',
  [[2, 'b'], [3, 'c']],
);
```

The native wire sends all rows in one request and returns per-row counts. Outside a transaction each row commits on its own. A failed row rejects with `BatchExecuteError`. Its `rowCounts` shows which rows committed. Use a transaction for all-or-nothing.

### Transactions

```typescript
await db.transaction(async tx => {
  await tx.execute('UPDATE items SET name = $1 WHERE id = $2', ['updated', 2]);
  await tx.execute('DELETE FROM items WHERE id = $1', [3]);
});
```

The callback runs on one connection. Statements run in order, including ones issued with `Promise.all`. A thrown error rolls back. The context expires when the callback returns, so await every statement. A COMMIT the server answers with a rollback rejects with `TransactionRolledBackError`.

### Errors

| Class | Meaning |
|-------|---------|
| `QueryError` | The server rejected the statement. `sqlstate` holds the SQLSTATE (`23505`, `40001`, `42P01`, ...). |
| `BatchExecuteError` | A batch row failed. Extends `QueryError`. `rowCounts` is `null` for each row that failed or did not run. `errors` lists the failed rows. |
| `TransactionRolledBackError` | COMMIT rolled back a failed block. Extends `QueryError`. |
| `ConnectionError` | Transport failure. |
| `DiskFullError` | The server is out of disk. |
| `PdbError` | Base class. |

A timeout or lost connection after a request was sent has an unknown outcome. The connection is closed and the request is not replayed. Only opening a new connection is retried. `PdbWireClient` exposes `maxRetries` and `retryDelayMs` for that.

### Key-value

```typescript
await db.kv.set('session:123', 'active', 3600);
const value = await db.kv.get('session:123');
await db.kv.delete('session:123');
```

`kv` also has `mset`, `mget`, `incr`, `exists`, `ttl` and `keys`.

### Vector search

```typescript
await db.vector.insert('docs', { title: 'a' }, 'embedding', [0.1, 0.2, 0.3]);
await db.vector.createIndex('docs', 'embedding', 'hnsw');
const hits = await db.vector.search('docs', 'embedding', [0.1, 0.2, 0.3], { k: 5 });
```

`search` orders by L2 distance (`<->`) and returns `{ row, distance }`.
The query vector and `k` are sent as bound parameters. Vectors must be non-empty arrays of finite numbers and `k` a positive integer. Table, column, `data`-key and index-option names must be unquoted SQL identifiers (`schema.table` is allowed for tables). Other values throw `TypeError` or `RangeError` before anything is sent. `createIndex` options accept numbers, booleans and strings. `filter` is a SQL `WHERE` fragment and is sent as written. Do not build it from untrusted input.

### Driver layer

These native-wire methods back the Drizzle (`@particledb/drizzle`) and Prisma (`@particledb/prisma-adapter`) adapters.

| Method | Purpose |
|--------|---------|
| `queryArrays(sql, params, { timestamps })` | Positional rows plus `fields[i].type` (Arrow type) and `fields[i].decl` (declared SQL type). `timestamps: 'string'` returns exact UTC text with microseconds. |
| `reserve()` | Checks out one connection for statements you sequence (`BEGIN`, `COMMIT`, `SAVEPOINT`). `release()` rolls back an open block. A COMMIT answered by rollback has `rolledBack: true`. |

### Limits

`query` and `stream` load the full result into memory. Use SQL pagination for large results. Each pooled connection carries one request at a time. The client does not expose multiplexing or server-side cancellation.

## Compatibility

| Component | Version |
|-----------|---------|
| ParticleDB server | 1.1.19-rc.15 or later |
| Node.js | 20 or later (22 and 24 tested) |
| `pg` (optional) | 8.11 or later, for `protocol: 'pg'` |

## Testing

```sh
npm install
npm run build
npm test
```

Live tests skip unless their variables are set. Run them against a disposable `--no-auth` server:

```sh
PDB_NATIVE_PORT=7173 npm test
PDB_NATIVE_PORT=7173 PDB_TLS_PORT=TLS_PORT PDB_TLS_CA=ca.pem \
  PDB_AUTH_PORT=AUTH_PORT PDB_AUTH_USER=app PDB_AUTH_PASS=secret npm test
```

Cross-SDK conformance:

```sh
PDB_BIN=/path/to/particledb conformance/run_conformance.sh
```

See [tests](tests/README.md) and [conformance](conformance/README.md).

## Links

- https://particledb.ai/sdks/overview/
- https://particledb.ai/sdks/typescript/

## License

MIT. See [LICENSE](LICENSE).

The ParticleDB server is under the ParticleDB License: https://particledb.ai/license/
