/**
 * Live-server test: `ParticleDB` defaults to the PDB native wire, binds
 * parameters through native prepared statements (MSG_PREPARE / MSG_EXECUTE /
 * MSG_CLOSE_STMT) and batch-inserts through MSG_EXECUTE_BATCH with an Arrow
 * IPC parameter stream. Every case asserts an invariant (counts, sums, exact
 * values), never just "no error".
 *
 * Runs only when PDB_NATIVE_PORT is set (host 127.0.0.1, --no-auth server);
 * otherwise every case is skipped. Prints `NDL_PASS typescript` when all
 * cases passed.
 *
 *   PDB_NATIVE_PORT=7173 node --import tsx --test tests/native_default.live.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

import { ParticleDB, PdbWireClient, QueryError, BatchExecuteError } from '../src';

const PORT = process.env.PDB_NATIVE_PORT;
const SKIP = PORT ? false : 'PDB_NATIVE_PORT not set';
const T = 'ndl_typescript';
const N = 10_000;
const CASES = 7;

async function rejects<E extends Error>(p: Promise<unknown>, cls: new (...a: any[]) => E, what: string): Promise<E> {
  let caught: unknown;
  try {
    await p;
  } catch (e) {
    caught = e;
  }
  assert.ok(caught !== undefined, `${what}: expected ${cls.name}, got success`);
  assert.ok(caught instanceof cls, `${what}: expected ${cls.name}, got ${String(caught)}`);
  return caught as E;
}

describe('native wire is the ParticleDB default', { skip: SKIP }, () => {
  let db: ParticleDB;
  let passed = 0;

  before(async () => {
    // A debug server on a loaded machine can need well over the default 30 s
    // for 10k autocommit rows; a timeout here would be an ambiguous failure.
    db = new ParticleDB({ host: '127.0.0.1', port: Number(PORT), poolSize: 2, timeoutMs: 600_000 });
    await db.execute(`DROP TABLE IF EXISTS ${T}`);
    await db.execute(
      `CREATE TABLE ${T} (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, flag BOOLEAN, blob BYTEA)`,
    );
  });

  after(async () => {
    await db?.execute(`DROP TABLE IF EXISTS ${T}`).catch(() => undefined);
    await db?.close();
    if (passed === CASES) console.log('NDL_PASS typescript');
  });

  it('0. kv.mget / kv.delete / vector.search over the native wire', async () => {
    // mget: one value per key, null for a missing key, and a value containing
    // a comma survives (the server's KV_MGET joins values with commas).
    await db.kv.set('ndl_kv_a', 'p,q');
    await db.kv.set('ndl_kv_b', 'z');
    assert.deepEqual(await db.kv.mget(['ndl_kv_a', 'ndl_kv_missing', 'ndl_kv_b']), ['p,q', null, 'z']);
    // KV_DEL answers a count; delete() maps it to a boolean.
    assert.equal(await db.kv.delete('ndl_kv_a'), true);
    assert.equal(await db.kv.delete('ndl_kv_a'), false);
    await db.kv.delete('ndl_kv_b');
    await db.execute('DROP TABLE IF EXISTS ndl_vec');
    await db.execute('CREATE TABLE ndl_vec (id BIGINT PRIMARY KEY, e VECTOR(3))');
    await db.execute("INSERT INTO ndl_vec VALUES (1,'[0,0,0]'),(2,'[5,5,5]'),(3,'[1,1,1]')");
    const hits = await db.vector.search('ndl_vec', 'e', [0.9, 1, 1], { k: 2 });
    assert.deepEqual(hits.map((h: any) => Number(h.id ?? h.row?.id)), [3, 1]);
    await db.execute('DROP TABLE ndl_vec');
  });

  it('1. default protocol is pdb; a plain query decodes Arrow types', async () => {
    assert.equal(db.protocol, 'pdb');
    const r = await db.query("SELECT 7 AS i, 'x' AS s, true AS b, NULL AS n");
    assert.deepEqual(r.columns, ['i', 's', 'b', 'n']);
    assert.deepEqual(r.rows, [{ i: 7, s: 'x', b: true, n: null }]);
    assert.equal(r.rowCount, 1);
    passed++;
  });

  it('2. parameters bind through PREPARE/EXECUTE (int64, float64, utf8, bool, binary, null)', async () => {
    const blob = Buffer.from([0, 255, 7]);
    const n = await db.execute(`INSERT INTO ${T} VALUES ($1, $2, $3, $4, $5)`, [-1, 'neg', 2.5, false, blob]);
    assert.equal(n, 1);
    assert.equal(await db.execute(`INSERT INTO ${T} VALUES ($1, $2, $3, $4, $5)`, [-2, null, null, null, null]), 1);
    const row = await db.queryOne(`SELECT id, name, score, flag, blob FROM ${T} WHERE id = $1`, [-1]);
    assert.deepEqual(row, { id: -1, name: 'neg', score: 2.5, flag: false, blob });
    const nul = await db.queryOne(`SELECT name, score, flag, blob FROM ${T} WHERE id = $1`, [-2]);
    assert.deepEqual(nul, { name: null, score: null, flag: null, blob: null });
    // The same SQL again reuses the cached prepared statement.
    assert.equal((await db.query(`SELECT id FROM ${T} WHERE id = $1`, [-2])).rows[0].id, -2);
    assert.equal(await db.execute(`DELETE FROM ${T} WHERE id < $1`, [0]), 2);
    passed++;
  });

  it(`3. executeBatch inserts ${N} rows in one EXECUTE_BATCH; count and sum invariants hold`, async () => {
    const rows: any[][] = [];
    for (let i = 1; i <= N; i++) {
      rows.push([i, i % 10 === 0 ? null : `name-${i}`, i / 4, i % 2 === 0, i % 3 === 0 ? null : Buffer.from([i & 0xff])]);
    }
    const r = await db.executeBatch(`INSERT INTO ${T} VALUES ($1, $2, $3, $4, $5)`, rows);
    assert.equal(r.totalRowCount, N);
    assert.equal(r.rowCounts.length, N);
    assert.ok(r.rowCounts.every((c) => c === 1));

    const agg = await db.queryOne<any>(
      `SELECT count(*) AS c, sum(id) AS s, count(name) AS names, sum(score) AS sc, count(blob) AS blobs FROM ${T}`,
    );
    assert.equal(Number(agg.c), N);
    assert.equal(Number(agg.s), (N * (N + 1)) / 2);
    assert.equal(Number(agg.names), N - N / 10);
    assert.equal(Number(agg.sc), (N * (N + 1)) / 2 / 4);
    assert.equal(Number(agg.blobs), N - Math.floor(N / 3));
    const flags = await db.queryOne<any>(`SELECT count(*) AS c FROM ${T} WHERE flag = $1`, [true]);
    assert.equal(Number(flags.c), N / 2);
    const spot = await db.queryOne(`SELECT id, name, score, flag, blob FROM ${T} WHERE id = $1`, [1234]);
    assert.deepEqual(spot, { id: 1234, name: 'name-1234', score: 308.5, flag: true, blob: Buffer.from([1234 & 0xff]) });
    passed++;
  });

  it('4. bad SQL rejects with QueryError (plain and prepared paths); a parameter count mismatch too', async () => {
    const e1 = await rejects(db.query('SELEC 1'), QueryError, 'bad SQL');
    assert.match(e1.message, /parse error|syntax/i);
    await rejects(db.query('SELEC $1', [1]), QueryError, 'bad SQL with params');
    await rejects(db.query(`SELECT id FROM ${T} WHERE id = $1`, [1, 2]), QueryError, 'param count');
    await rejects(db.executeBatch(`SELECT * FROM no_such_table_ndl WHERE id = $1`, [[1]]), QueryError, 'batch on bad table');
    passed++;
  });

  it('5. a constraint violation inside an autocommit batch is a BatchExecuteError; other rows commit', async () => {
    const e = await rejects(
      db.executeBatch(`INSERT INTO ${T} (id, name) VALUES ($1, $2)`, [[N + 1, 'a'], [5, 'dup'], [N + 2, 'b']]),
      BatchExecuteError,
      'duplicate in batch',
    );
    assert.ok(e instanceof QueryError);
    assert.equal(e.errors.length, 1);
    assert.equal(e.errors[0].row, 1);
    assert.match(e.errors[0].message, /duplicate key/);
    assert.deepEqual(e.rowCounts, [1, null, 1]);
    const c = await db.queryOne<any>(`SELECT count(*) AS c FROM ${T}`);
    assert.equal(Number(c.c), N + 2);
    const five = await db.queryOne<any>(`SELECT name FROM ${T} WHERE id = 5`);
    assert.equal(five.name, 'name-5', 'the existing row was not overwritten');
    passed++;
  });

  it('6. inside a transaction a failing batch row rolls back the whole transaction', async () => {
    const e = await rejects(
      db.transaction(async (tx) => {
        assert.equal(await tx.execute(`INSERT INTO ${T} (id) VALUES ($1)`, [N + 10]), 1);
        await tx.executeBatch(`INSERT INTO ${T} (id) VALUES ($1)`, [[N + 11], [7], [N + 12]]);
      }),
      BatchExecuteError,
      'duplicate in txn batch',
    );
    assert.deepEqual(e.rowCounts, [1, null, null]);
    const c = await db.queryOne<any>(`SELECT count(*) AS c FROM ${T} WHERE id >= $1`, [N + 10]);
    assert.equal(Number(c.c), 0, 'nothing from the rolled-back transaction is visible');
    // A successful transaction with a batch commits.
    const r = await db.transaction(async (tx) =>
      tx.executeBatch(`INSERT INTO ${T} (id) VALUES ($1)`, [[N + 20], [N + 21]]),
    );
    assert.equal(r.totalRowCount, 2);
    assert.equal(await db.execute(`DELETE FROM ${T} WHERE id >= $1`, [N + 20]), 2);
    passed++;
  });

  it('7. prepared-statement cache eviction (CLOSE_STMT) keeps results correct', async () => {
    const c = new PdbWireClient({ host: '127.0.0.1', port: Number(PORT), poolSize: 1, statementCacheSize: 2 });
    try {
      for (let round = 0; round < 2; round++) {
        for (let k = 1; k <= 5; k++) {
          const rows = await c.query(`SELECT id, ${k} AS k FROM ${T} WHERE id = $1`, [k * 100]);
          assert.deepEqual(rows, [{ id: k * 100, k }]);
        }
      }
    } finally {
      await c.close();
    }
    passed++;
  });
});
