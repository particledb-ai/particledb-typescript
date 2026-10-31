/**
 * Live SQL-injection regressions for `VectorOps` against a real server: each
 * hostile payload is stored as data or refused, and a canary table survives.
 *
 * Runs only when PDB_NATIVE_PORT is set (host 127.0.0.1, --no-auth server).
 *
 *   PDB_NATIVE_PORT=7173 node --import tsx --test tests/vector_injection.live.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

import { ParticleDB } from '../src';

const PORT = process.env.PDB_NATIVE_PORT;
const SKIP = PORT ? false : 'PDB_NATIVE_PORT not set';
const T = 'ts_inj_vec';
const CANARY = 'ts_inj_canary';

const PAYLOADS = [
  `'); DROP TABLE ${CANARY}; --`,
  "\\'",
  `1]'); DROP TABLE ${CANARY}; --`,
  '{"a":"it\'s \\\\ \\"q\\""}',
  'é✓ 漢字 \u{1F600}',
];

describe('VectorOps injection (live)', { skip: SKIP }, () => {
  let db: ParticleDB;

  const canaryAlive = async (): Promise<void> => {
    const r = await db.query(`SELECT count(*) AS n FROM ${CANARY}`);
    assert.equal(Number(r.rows[0].n), 1, 'canary table must survive with its row');
  };

  before(async () => {
    db = new ParticleDB({ host: '127.0.0.1', port: Number(PORT), poolSize: 2 });
    await db.execute(`DROP TABLE IF EXISTS ${T}`);
    await db.execute(`DROP TABLE IF EXISTS ${CANARY}`);
    await db.execute(`CREATE TABLE ${CANARY} (id BIGINT PRIMARY KEY)`);
    await db.execute(`INSERT INTO ${CANARY} VALUES (1)`);
    await db.execute(`CREATE TABLE ${T} (id BIGINT PRIMARY KEY, title TEXT, embedding VECTOR(3))`);
  });
  after(async () => {
    await db.execute(`DROP TABLE IF EXISTS ${T}`).catch(() => undefined);
    await db.execute(`DROP TABLE IF EXISTS ${CANARY}`).catch(() => undefined);
    await db.close();
  });

  it('insert stores every payload as data, byte for byte', async () => {
    for (const [i, p] of PAYLOADS.entries()) {
      await db.vector.insert(T, { id: i + 1, title: p }, 'embedding', [i, 0, 1]);
    }
    const r = await db.query(`SELECT id, title FROM ${T} ORDER BY id`);
    assert.deepEqual(r.rows.map((row) => row.title), PAYLOADS);
    await canaryAlive();
  });

  it('search finds by bound vector and k, and refuses hostile vector / k', async () => {
    const hits = await db.vector.search(T, 'embedding', [0, 0, 1], { k: 2 });
    assert.equal(hits.length, 2);
    assert.equal(hits[0].row.title, PAYLOADS[0]);
    assert.equal(hits[0].distance, 0);
    for (const p of PAYLOADS) {
      await assert.rejects(db.vector.search(T, 'embedding', [p as any, 0, 1]), TypeError);
      await assert.rejects(db.vector.search(T, 'embedding', [0, 0, 1], { k: p as any }), RangeError);
    }
    await assert.rejects(db.vector.search(T, 'embedding', [0, 0, 1], { k: `1; DROP TABLE ${CANARY}` as any }), RangeError);
    await assert.rejects(db.vector.search(`${T}; DROP TABLE ${CANARY}; --`, 'embedding', [0, 0, 1]), TypeError);
    await canaryAlive();
  });

  it('insert refuses a hostile vector and identifiers', async () => {
    await assert.rejects(db.vector.insert(T, { id: 99 }, 'embedding', [`1]'); DROP TABLE ${CANARY}; --` as any]), TypeError);
    await assert.rejects(db.vector.insert(T, { [`id) VALUES (98); DROP TABLE ${CANARY}; --`]: 1 }, 'embedding', [0, 0, 0]), TypeError);
    const r = await db.query(`SELECT count(*) AS n FROM ${T}`);
    assert.equal(Number(r.rows[0].n), PAYLOADS.length);
    await canaryAlive();
  });

  it('createIndex sends a string option as one literal the server reads as a value', async () => {
    for (const p of PAYLOADS) {
      // The server refuses a non-integer `m`; what matters is that it parsed the
      // payload as the option's VALUE and ran nothing else.
      await assert.rejects(db.vector.createIndex(T, 'embedding', 'hnsw', { m: p }), (e: Error) => {
        assert.match(e.message, /invalid value for integer option "m"/);
        assert.ok(e.message.includes(p), `server must echo the payload as a value: ${e.message}`);
        return true;
      });
      await canaryAlive();
    }
    await assert.rejects(db.vector.createIndex(T, 'embedding', 'hnsw', { [`m = 1); DROP TABLE ${CANARY}; --`]: 1 }), TypeError);
    await db.vector.createIndex(T, 'embedding', 'hnsw', { m: 16, ef_construction: 64 });
    await canaryAlive();
  });
});
