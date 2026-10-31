/**
 * Live-server regressions for native-wire transaction / batch / decode edges.
 * Every case asserts the database state, not just the error.
 *
 * Runs only when PDB_NATIVE_PORT is set (host 127.0.0.1, --no-auth server).
 *
 *   PDB_NATIVE_PORT=7173 node --import tsx --test tests/native_regress.live.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

import { ParticleDB, PdbWireClient, TransactionRolledBackError } from '../src';

const PORT = process.env.PDB_NATIVE_PORT;
const SKIP = PORT ? false : 'PDB_NATIVE_PORT not set';
const T = 'nrg_typescript';

describe('native wire regressions', { skip: SKIP }, () => {
  let db: ParticleDB;
  let wire: PdbWireClient;

  const count = async (): Promise<number> => Number((await db.query(`SELECT count(*) AS n FROM ${T}`)).rows[0].n);

  before(async () => {
    db = new ParticleDB({ host: '127.0.0.1', port: Number(PORT), poolSize: 2 });
    wire = new PdbWireClient({ host: '127.0.0.1', port: Number(PORT), poolSize: 2 });
    await db.execute(`DROP TABLE IF EXISTS ${T}`);
    await db.execute(`CREATE TABLE ${T} (id BIGINT PRIMARY KEY, v TEXT)`);
  });
  after(async () => {
    await db.execute(`DROP TABLE IF EXISTS ${T}`).catch(() => undefined);
    await db.close();
    await wire.close();
  });

  it('a statement error caught inside transaction() makes it reject; nothing commits', async () => {
    await db.execute(`DELETE FROM ${T}`);
    let caught: unknown;
    try {
      await db.transaction(async (tx) => {
        await tx.execute(`INSERT INTO ${T} VALUES ($1, $2)`, [1, 'a']);
        await tx.execute(`INSERT INTO ${T} VALUES ($1, $2)`, [1, 'dup']).catch(() => undefined);
      });
    } catch (e) {
      caught = e;
    }
    assert.ok(caught instanceof TransactionRolledBackError, `expected TransactionRolledBackError, got ${String(caught)}`);
    assert.equal(await count(), 0);
  });

  it('a zero-parameter executeBatch runs once per row', async () => {
    await db.execute(`DELETE FROM ${T}`);
    await db.execute(`CREATE TABLE IF NOT EXISTS ${T}_seq (v INT)`);
    await db.execute(`DELETE FROM ${T}_seq`);
    try {
      const r = await wire.executeBatch(`INSERT INTO ${T}_seq VALUES (1)`, [[], [], []]);
      assert.deepEqual(r.rowCounts, [1, 1, 1]);
      assert.equal(Number((await db.query(`SELECT count(*) AS n FROM ${T}_seq`)).rows[0].n), 3);
    } finally {
      await db.execute(`DROP TABLE IF EXISTS ${T}_seq`);
    }
  });

  it('an oversized last row is refused before any frame is sent', async () => {
    await db.execute(`DELETE FROM ${T}`);
    await assert.rejects(
      wire.executeBatch(`INSERT INTO ${T} VALUES ($1, $2)`, [[1, 'ok'], [2, 'x'.repeat(17 * 1024 * 1024)]]),
      RangeError,
    );
    assert.equal(await count(), 0);
  });

  it('a bigint above 2^53 in a float-promoted column is refused, not rounded', async () => {
    await db.execute(`DELETE FROM ${T}`);
    await assert.rejects(
      wire.executeBatch(`INSERT INTO ${T} VALUES ($1, $2)`, [[9007199254740993n, 'a'], [0.5, 'b']]),
      RangeError,
    );
    assert.equal(await count(), 0);
  });

  it('a Decimal256 result (numeric over 38 digits) decodes exactly', async () => {
    const big = '123456789012345678901234567890123456789012';
    const r = await db.query(`SELECT ${big}::numeric AS big, -${big}::numeric AS neg`);
    assert.equal(r.rows[0].big, big);
    assert.equal(r.rows[0].neg, '-' + big);
  });
});
