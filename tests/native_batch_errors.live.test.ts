/**
 * Live-server test: a failed statement inside a native-wire BATCH or
 * transaction must surface as an error, never as success.
 *
 * Runs only when PDB_NATIVE_PORT is set (host 127.0.0.1, --no-auth server);
 * otherwise every case is skipped. Prints `NBE_PASS typescript` when all
 * cases pass, so a runner can prove the test actually ran.
 *
 *   PDB_NATIVE_PORT=7173 node --import tsx --test tests/native_batch_errors.live.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

import { PdbWireClient, QueryError } from '../src/pdb_wire';

const PORT = process.env.PDB_NATIVE_PORT;
const SKIP = PORT ? false : 'PDB_NATIVE_PORT not set';
const T = 'nbe_typescript';

async function expectDuplicateKey(p: Promise<unknown>, what: string): Promise<void> {
  let caught: unknown;
  try {
    await p;
  } catch (e) {
    caught = e;
  }
  assert.ok(caught !== undefined, `${what}: expected a duplicate-key error, got success`);
  assert.ok(caught instanceof QueryError, `${what}: expected QueryError, got ${String(caught)}`);
  assert.match((caught as Error).message, /duplicate key/, `${what}: wrong message`);
}

describe('native wire: batch / transaction errors are raised', { skip: SKIP }, () => {
  let db: PdbWireClient;
  let passed = 0;

  before(async () => {
    db = new PdbWireClient({ host: '127.0.0.1', port: Number(PORT), poolSize: 1, maxRetries: 0 });
    await db.execute(`DROP TABLE IF EXISTS ${T}`);
    await db.execute(`CREATE TABLE ${T} (id INT PRIMARY KEY)`);
  });

  after(async () => {
    await db?.close();
    if (passed === 5) console.log('NBE_PASS typescript');
  });

  it('a. batch [INSERT k1, INSERT k1, INSERT k2] raises', async () => {
    await expectDuplicateKey(
      db.batch([
        `INSERT INTO ${T} VALUES (1)`,
        `INSERT INTO ${T} VALUES (1)`,
        `INSERT INTO ${T} VALUES (2)`,
      ]),
      'batch',
    );
    passed++;
  });

  it('b. transaction [INSERT k3, INSERT k3] raises', async () => {
    await expectDuplicateKey(
      db.transaction(async (tx) => {
        await tx.execute(`INSERT INTO ${T} VALUES (3)`);
        await tx.execute(`INSERT INTO ${T} VALUES (3)`);
      }),
      'transaction',
    );
    passed++;
  });

  it('c. the same pooled connection is usable after the failed transaction', async () => {
    const n = await db.execute(`INSERT INTO ${T} VALUES (4)`);
    assert.equal(n, 1);
    // k3 was rolled back, so it is insertable again.
    assert.equal(await db.execute(`INSERT INTO ${T} VALUES (3)`), 1);
    passed++;
  });

  it('d. successful batch and transaction still succeed and commit', async () => {
    await db.batch([`INSERT INTO ${T} VALUES (10)`, `INSERT INTO ${T} VALUES (11)`]);
    await db.transaction(async (tx) => {
      await tx.execute(`INSERT INTO ${T} VALUES (12)`);
    });
    for (const k of [10, 11, 12]) {
      assert.equal(await db.execute(`DELETE FROM ${T} WHERE id = ${k}`), 1, `row ${k} visible`);
    }
    passed++;
  });

  it('e. a failed statement after a raw BEGIN does not poison the pool', async () => {
    // BEGIN through the single-statement API, then a failing statement: that
    // pooled connection is inside a FAILED transaction and must not be handed
    // out again.
    await db.execute('BEGIN');
    await expectDuplicateKey(db.execute(`INSERT INTO ${T} VALUES (4)`), 'statement after BEGIN');
    assert.equal(await db.execute(`INSERT INTO ${T} VALUES (40)`), 1);
    assert.equal(await db.execute(`DELETE FROM ${T} WHERE id = 40`), 1, 'the next statement committed');
    passed++;
  });
});
