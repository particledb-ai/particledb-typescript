/**
 * The driver-layer surface ORM adapters build on: SQLSTATE decoding
 * (`QueryError.sqlstate`), `queryArrays` (positional rows + column
 * descriptions, exact timestamps) and `reserve` (one dedicated connection).
 * The SQLSTATE part is pure; the rest runs only when PDB_NATIVE_PORT is set
 * (host 127.0.0.1, --no-auth server).
 *
 *   PDB_NATIVE_PORT=7173 node --import tsx --test tests/driver_surface.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

import { ParticleDB, PdbWireClient, QueryError, sqlstateForCode, sqlstateOf } from '../src';

describe('SQLSTATE decoding', () => {
  it('maps numeric, banded and protocol codes', () => {
    assert.equal(sqlstateForCode(23505), '23505');
    assert.equal(sqlstateForCode(60001), '42P01');
    assert.equal(sqlstateForCode(60003), '25P02');
    assert.equal(sqlstateForCode(2000), '02000');
    assert.equal(sqlstateForCode(0x07), '57014');
    assert.equal(sqlstateForCode(1064), 'HY000');
    assert.equal(sqlstateForCode(65000), 'HY000');
  });
  it('prefers an exact [XXXXX] marker, then the code, then a XXXXX: prefix', () => {
    assert.equal(sqlstateOf(40001, 'could not serialize [40001]'), '40001');
    assert.equal(sqlstateOf(1064, 'KV_INCR: [22003] value out of range'), '22003');
    assert.equal(sqlstateOf(1064, '0A000: not supported'), '0A000');
    assert.equal(sqlstateOf(60001, 'table not found: t'), '42P01');
  });
});

const PORT = process.env.PDB_NATIVE_PORT;
const SKIP = PORT ? false : 'PDB_NATIVE_PORT not set';
const T = 'drv_surface_typescript';

describe('driver surface (live)', { skip: SKIP }, () => {
  let wire: PdbWireClient;
  let db: ParticleDB;

  before(async () => {
    wire = new PdbWireClient({ host: '127.0.0.1', port: Number(PORT), poolSize: 2, transport: 'tcp' });
    db = new ParticleDB({ host: '127.0.0.1', port: Number(PORT), poolSize: 2, transport: 'tcp' });
    await wire.execute(`DROP TABLE IF EXISTS ${T}`);
    await wire.execute(`CREATE TABLE ${T} (id INT PRIMARY KEY, n NUMERIC(10,2), ts TIMESTAMP(6), tz TIMESTAMPTZ, j JSONB, a INT[])`);
  });
  after(async () => {
    await wire.execute(`DROP TABLE IF EXISTS ${T}`).catch(() => undefined);
    await wire.close();
    await db.close();
  });

  it('queryArrays keeps same-named columns apart and describes each column', async () => {
    await wire.execute(`INSERT INTO ${T} VALUES (1, 12.30, '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05+00', '{"a": 1}', '{1,NULL,3}')`);
    const r = await db.queryArrays(`SELECT t.id, t.id AS id, n, ts, tz, j, a FROM ${T} t`, undefined, { timestamps: 'string' });
    assert.deepEqual(r.columns, ['id', 'id', 'n', 'ts', 'tz', 'j', 'a']);
    assert.deepEqual(r.rows, [[1, 1, '12.30', '2024-01-02 03:04:05.123456', '2024-01-02 03:04:05', '{"a": 1}', '{1,NULL,3}']]);
    assert.equal(r.hasResultSet, true);
    assert.deepEqual(r.fields.map((f) => f.type), ['int64', 'int64', 'decimal', 'timestamp', 'timestamp', 'utf8', 'utf8']);
    assert.match(String(r.fields[4].decl), /timestamptz/i);
    assert.match(String(r.fields[6].decl), /\[\]$/);
    const d = await wire.queryArrays(`SELECT ts FROM ${T}`);
    assert.ok(d.rows[0][0] instanceof Date, 'default decode is Date');
  });

  it('queryArrays reports affected rows for DML', async () => {
    const r = await wire.queryArrays(`UPDATE ${T} SET n = n + 1 WHERE id = $1`, [1]);
    assert.equal(r.hasResultSet, false);
    assert.equal(r.rowCount, 1);
  });

  it('QueryError carries the SQLSTATE', async () => {
    const err = await wire.queryArrays(`INSERT INTO ${T} (id) VALUES (1)`).catch((e) => e);
    assert.ok(err instanceof QueryError);
    assert.equal(err.sqlstate, '23505');
    const missing = await wire.queryArrays('SELECT * FROM no_such_table_ts').catch((e) => e);
    assert.equal(missing.sqlstate, '42P01');
  });

  it('reserve: one session for BEGIN..COMMIT; a rolled-back COMMIT is flagged', async () => {
    const conn = await wire.reserve();
    try {
      await conn.queryArrays('BEGIN');
      assert.equal(conn.inTransaction, true);
      await conn.queryArrays(`INSERT INTO ${T} (id) VALUES (2)`);
      const outside = await db.queryArrays(`SELECT count(*) FROM ${T} WHERE id = 2`);
      assert.equal(Number(outside.rows[0][0]), 0, 'uncommitted row invisible elsewhere');
      const c = await conn.queryArrays('COMMIT');
      assert.equal(c.rolledBack, false);
      assert.equal(conn.inTransaction, false);

      await conn.queryArrays('BEGIN');
      await conn.queryArrays(`INSERT INTO ${T} (id) VALUES (3)`);
      await conn.queryArrays(`INSERT INTO ${T} (id) VALUES (3)`).catch(() => undefined);
      const rb = await conn.queryArrays('COMMIT');
      assert.equal(rb.rolledBack, true, 'COMMIT of a failed block answers ROLLBACK');
    } finally {
      await conn.release();
    }
    const n = await db.queryArrays(`SELECT count(*) FROM ${T} WHERE id IN (2, 3)`);
    assert.equal(Number(n.rows[0][0]), 1);
  });

  it('release() rolls back a block left open', async () => {
    const conn = await wire.reserve();
    await conn.queryArrays('BEGIN');
    await conn.queryArrays(`INSERT INTO ${T} (id) VALUES (4)`);
    await conn.release();
    await assert.rejects(conn.queryArrays('SELECT 1'), /released/);
    const n = await db.queryArrays(`SELECT count(*) FROM ${T} WHERE id = 4`);
    assert.equal(Number(n.rows[0][0]), 0);
    // The pooled connection is clean: a new statement is not inside a block.
    const again = await wire.reserve();
    try {
      await again.queryArrays(`INSERT INTO ${T} (id) VALUES (5)`);
    } finally {
      await again.release();
    }
    assert.equal(Number((await db.queryArrays(`SELECT count(*) FROM ${T} WHERE id = 5`)).rows[0][0]), 1);
  });
});
