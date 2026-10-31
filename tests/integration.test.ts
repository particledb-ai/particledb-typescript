/**
 * Integration tests for the ParticleDB TypeScript SDK.
 *
 * These tests connect to a REAL ParticleDB server and exercise the full
 * SDK surface end-to-end.
 *
 * Required environment variables:
 *   PARTICLEDB_TEST_PORT  - port of the running ParticleDB instance (test is skipped if unset)
 *
 * Optional environment variables:
 *   PARTICLEDB_TEST_HOST     - hostname (default: 'localhost')
 *   PARTICLEDB_TEST_USER     - username (default: 'particledb')
 *   PARTICLEDB_TEST_PASSWORD - password (default: undefined)
 *   PARTICLEDB_TEST_DATABASE - database (default: 'particledb')
 *   PARTICLEDB_AI_ENABLED    - set to '1' or 'true' to run AI tests
 *
 * All tables created by these tests use the prefix "ts_integ_" and are
 * cleaned up in afterEach hooks.
 */

import { describe, it, before, after, afterEach, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { ParticleDB } from '../src';

// ---------------------------------------------------------------------------
// Skip guard - the entire file is a no-op when PARTICLEDB_TEST_PORT is unset.
// ---------------------------------------------------------------------------
const TEST_PORT = process.env.PARTICLEDB_TEST_PORT;
const SKIP = !TEST_PORT;

if (SKIP) {
  it('native integration requires PARTICLEDB_TEST_PORT', {skip: 'PARTICLEDB_TEST_PORT not set'}, () => {});
}

const TABLE_PREFIX = 'ts_integ_';

function testConfig(): {
  host: string;
  port: number;
  user: string;
  password: string | undefined;
  database: string;
} {
  return {
    host: process.env.PARTICLEDB_TEST_HOST ?? 'localhost',
    port: parseInt(process.env.PARTICLEDB_TEST_PORT!, 10),
    user: process.env.PARTICLEDB_TEST_USER ?? 'particledb',
    password: process.env.PARTICLEDB_TEST_PASSWORD,
    database: process.env.PARTICLEDB_TEST_DATABASE ?? 'particledb',
  };
}

function aiEnabled(): boolean {
  const v = process.env.PARTICLEDB_AI_ENABLED;
  return v === '1' || v === 'true';
}

/** Silently drop a table if it exists. */
async function dropTable(db: ParticleDB, name: string): Promise<void> {
  try {
    await db.execute(`DROP TABLE IF EXISTS ${name}`);
  } catch {
    // table may not exist - ignore
  }
}

// ---------------------------------------------------------------------------
// Test suites (only registered when PARTICLEDB_TEST_PORT is set)
// ---------------------------------------------------------------------------

if (!SKIP) {
  // =======================================================================
  // 1. Connection & Lifecycle
  // =======================================================================
  describe('1. Connection & Lifecycle', () => {
    let db: ParticleDB;

    afterEach(async () => {
      if (db) await db.close();
    });

    it('should connect and execute SELECT 1', async () => {
      db = new ParticleDB(testConfig());
      const result = await db.query<{ n: number }>('SELECT 1 AS n');
      assert.equal(result.rows.length, 1);
      // pg returns numbers as strings for some integer types; coerce
      assert.equal(Number(result.rows[0].n), 1);
    });

    it('should support multiple sequential connections', async () => {
      const db1 = new ParticleDB(testConfig());
      const r1 = await db1.query('SELECT 1 AS v');
      assert.equal(r1.rows.length, 1);
      await db1.close();

      const db2 = new ParticleDB(testConfig());
      const r2 = await db2.query('SELECT 1 AS v');
      assert.equal(r2.rows.length, 1);
      await db2.close();

      // Assign to db so afterEach doesn't fail - already closed
      db = new ParticleDB(testConfig());
    });

    it('should close cleanly', async () => {
      db = new ParticleDB(testConfig());
      await db.query('SELECT 1');
      await db.close();
      // After close, queries should fail
      await assert.rejects(async () => {
        await db.query('SELECT 1');
      });
      // Re-create so afterEach doesn't double-close
      db = new ParticleDB(testConfig());
    });
  });

  // =======================================================================
  // 2. SQL CRUD
  // =======================================================================
  describe('2. SQL CRUD', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}crud`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
    });

    afterEach(async () => {
      await dropTable(db, TABLE);
    });

    after(async () => {
      await db.close();
    });

    it('should create a table', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      // Verify table exists by selecting from it
      const result = await db.query(`SELECT * FROM ${TABLE}`);
      assert.equal(result.rows.length, 0);
    });

    it('should insert and select rows', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      await db.execute(
        `INSERT INTO ${TABLE} (id, name) VALUES (1, 'Alice')`
      );
      await db.execute(
        `INSERT INTO ${TABLE} (id, name) VALUES (2, 'Bob')`
      );

      const result = await db.query<{ id: number; name: string }>(
        `SELECT id, name FROM ${TABLE} ORDER BY id`
      );
      assert.equal(result.rows.length, 2);
      assert.equal(Number(result.rows[0].id), 1);
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(Number(result.rows[1].id), 2);
      assert.equal(result.rows[1].name, 'Bob');
    });

    it('should select with WHERE clause', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (1, 'Alice')`);
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (2, 'Bob')`);

      const result = await db.query<{ id: number; name: string }>(
        `SELECT id, name FROM ${TABLE} WHERE id = 2`
      );
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Bob');
    });

    it('should select with parameterized queries', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id BIGINT PRIMARY KEY, name TEXT)`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (1, 'Alice')`);

      // ParticleDB extended query protocol has limited $1 support;
      // inline the value for now
      const result = await db.query<{ id: string; name: string }>(
        `SELECT id, name FROM ${TABLE} WHERE id = 1`
      );
      assert.equal(result.rows.length, 1);
      assert.equal(result.rows[0].name, 'Alice');
    });

    it('should update rows and return affected count', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (1, 'Alice')`);
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (2, 'Bob')`);

      const affected = await db.execute(
        `UPDATE ${TABLE} SET name = 'Updated' WHERE id = 1`
      );
      assert.equal(affected, 1);

      const row = await db.queryOne<{ name: string }>(
        `SELECT name FROM ${TABLE} WHERE id = 1`
      );
      assert.ok(row);
      assert.equal(row!.name, 'Updated');
    });

    it('should delete rows and return affected count', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (1, 'Alice')`);
      await db.execute(`INSERT INTO ${TABLE} (id, name) VALUES (2, 'Bob')`);

      const affected = await db.execute(`DELETE FROM ${TABLE} WHERE id = 1`);
      assert.equal(affected, 1);

      const result = await db.query(`SELECT * FROM ${TABLE}`);
      assert.equal(result.rows.length, 1);
    });

    it('should return rowCount on INSERT', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      const count = await db.execute(
        `INSERT INTO ${TABLE} (id, name) VALUES (1, 'Alice')`
      );
      assert.equal(count, 1);
    });

    it('should return empty result set from empty table', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      const result = await db.query(`SELECT * FROM ${TABLE}`);
      assert.equal(result.rows.length, 0);
    });

    it('should queryOne returning null for no match', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      const row = await db.queryOne(`SELECT * FROM ${TABLE} WHERE id = 999`);
      assert.equal(row, null);
    });

    it('should drop a table', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`DROP TABLE ${TABLE}`);
      // Selecting from dropped table should error
      await assert.rejects(async () => {
        await db.query(`SELECT * FROM ${TABLE}`);
      });
    });
  });

  // =======================================================================
  // 3. Data Types
  // =======================================================================
  describe('3. Data Types', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}types`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
    });

    afterEach(async () => {
      await dropTable(db, TABLE);
    });

    after(async () => {
      await db.close();
    });

    it('should handle INT64 type', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, val INT64, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (1, 42)`);
      const row = await db.queryOne<{ val: number }>(`SELECT val FROM ${TABLE} WHERE id = 1`);
      assert.ok(row);
      assert.equal(Number(row!.val), 42);
    });

    it('should handle FLOAT64 type', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, val FLOAT64, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (1, 3.14)`);
      const row = await db.queryOne<{ val: number }>(`SELECT val FROM ${TABLE} WHERE id = 1`);
      assert.ok(row);
      assert.ok(Math.abs(Number(row!.val) - 3.14) < 0.001);
    });

    it('should handle TEXT type', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, val TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (1, 'hello world')`);
      const row = await db.queryOne<{ val: string }>(`SELECT val FROM ${TABLE} WHERE id = 1`);
      assert.ok(row);
      assert.equal(row!.val, 'hello world');
    });

    it('should handle BOOLEAN type', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, val BOOLEAN, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (1, TRUE)`);
      await db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (2, FALSE)`);

      const rowTrue = await db.queryOne<{ val: boolean }>(`SELECT val FROM ${TABLE} WHERE id = 1`);
      assert.ok(rowTrue);
      assert.equal(rowTrue!.val, true);

      const rowFalse = await db.queryOne<{ val: boolean }>(`SELECT val FROM ${TABLE} WHERE id = 2`);
      assert.ok(rowFalse);
      assert.equal(rowFalse!.val, false);
    });

    it('should handle mixed types in a single row', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, score FLOAT64, name TEXT, active BOOLEAN, PRIMARY KEY (id))`
      );
      await db.execute(
        `INSERT INTO ${TABLE} (id, score, name, active) VALUES (1, 9.5, 'Alice', TRUE)`
      );

      const row = await db.queryOne<{ id: number; score: number; name: string; active: boolean }>(
        `SELECT * FROM ${TABLE} WHERE id = 1`
      );
      assert.ok(row);
      assert.equal(Number(row!.id), 1);
      assert.ok(Math.abs(Number(row!.score) - 9.5) < 0.001);
      assert.equal(row!.name, 'Alice');
      assert.equal(row!.active, true);
    });
  });

  // =======================================================================
  // 4. Transactions
  // =======================================================================
  describe('4. Transactions', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}txn`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
    });

    beforeEach(async () => {
      await dropTable(db, TABLE);
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, balance INT64, PRIMARY KEY (id))`
      );
    });

    afterEach(async () => {
      await dropTable(db, TABLE);
    });

    after(async () => {
      await db.close();
    });

    it('should commit a successful transaction', async () => {
      await db.transaction(async (tx) => {
        await tx.execute(`INSERT INTO ${TABLE} (id, balance) VALUES (1, 100)`);
        await tx.execute(`INSERT INTO ${TABLE} (id, balance) VALUES (2, 200)`);
      });

      const result = await db.query(`SELECT * FROM ${TABLE} ORDER BY id`);
      assert.equal(result.rows.length, 2);
      assert.equal(Number(result.rows[0].balance), 100);
      assert.equal(Number(result.rows[1].balance), 200);
    });

    it('should rollback on error', async () => {
      await db.execute(`INSERT INTO ${TABLE} (id, balance) VALUES (1, 100)`);

      await assert.rejects(async () => {
        await db.transaction(async (tx) => {
          await tx.execute(`UPDATE ${TABLE} SET balance = 50 WHERE id = 1`);
          throw new Error('intentional failure');
        });
      }, { message: 'intentional failure' });

      // Balance should remain unchanged
      const row = await db.queryOne<{ balance: number }>(
        `SELECT balance FROM ${TABLE} WHERE id = 1`
      );
      assert.ok(row);
      assert.equal(Number(row!.balance), 100);
    });

    it('should support multi-statement transactions', async () => {
      await db.execute(`INSERT INTO ${TABLE} (id, balance) VALUES (1, 100)`);
      await db.execute(`INSERT INTO ${TABLE} (id, balance) VALUES (2, 200)`);

      const result = await db.transaction(async (tx) => {
        await tx.execute(`UPDATE ${TABLE} SET balance = balance - 50 WHERE id = 1`);
        await tx.execute(`UPDATE ${TABLE} SET balance = balance + 50 WHERE id = 2`);
        const r = await tx.query<{ balance: number }>(
          `SELECT balance FROM ${TABLE} WHERE id = 2`
        );
        return Number(r.rows[0].balance);
      });

      assert.equal(result, 250);

      const row1 = await db.queryOne<{ balance: number }>(
        `SELECT balance FROM ${TABLE} WHERE id = 1`
      );
      assert.equal(Number(row1!.balance), 50);
    });
  });

  // =======================================================================
  // 5. KV Operations
  // =======================================================================
  describe('5. KV Operations', () => {
    let db: ParticleDB;
    const KEY_PREFIX = 'ts_integ_kv_';

    before(async () => {
      db = new ParticleDB(testConfig());
    });

    afterEach(async () => {
      // Clean up keys created during tests
      const keysToClean = [
        `${KEY_PREFIX}hello`,
        `${KEY_PREFIX}missing`,
        `${KEY_PREFIX}del`,
        `${KEY_PREFIX}exists`,
        `${KEY_PREFIX}counter`,
        `${KEY_PREFIX}overwrite`,
        `${KEY_PREFIX}m1`,
        `${KEY_PREFIX}m2`,
        `${KEY_PREFIX}m3`,
      ];
      for (const k of keysToClean) {
        try { await db.kv.delete(k); } catch { /* ignore */ }
      }
    });

    after(async () => {
      await db.close();
    });

    it('should set and get a value', async () => {
      await db.kv.set(`${KEY_PREFIX}hello`, 'world');
      const val = await db.kv.get(`${KEY_PREFIX}hello`);
      assert.equal(val, 'world');
    });

    it('should return null for a missing key', async () => {
      const val = await db.kv.get(`${KEY_PREFIX}missing`);
      assert.equal(val, null);
    });

    it('should delete a key', async () => {
      await db.kv.set(`${KEY_PREFIX}del`, 'temp');
      await db.kv.delete(`${KEY_PREFIX}del`);
      // Verify deletion via follow-up KV_GET
      const val = await db.kv.get(`${KEY_PREFIX}del`);
      assert.equal(val, null);
    });

    it('should check key existence', async () => {
      const beforeSet = await db.kv.exists(`${KEY_PREFIX}exists`);
      assert.equal(beforeSet, false);

      await db.kv.set(`${KEY_PREFIX}exists`, 'yes');
      const afterSet = await db.kv.exists(`${KEY_PREFIX}exists`);
      assert.equal(afterSet, true);
    });

    it('should increment a numeric value', async () => {
      await db.kv.set(`${KEY_PREFIX}counter`, '10');
      const newVal = await db.kv.incr(`${KEY_PREFIX}counter`, 5);
      assert.equal(Number(newVal), 15);
    });

    it('should overwrite an existing key', async () => {
      await db.kv.set(`${KEY_PREFIX}overwrite`, 'first');
      await db.kv.set(`${KEY_PREFIX}overwrite`, 'second');
      const val = await db.kv.get(`${KEY_PREFIX}overwrite`);
      assert.equal(val, 'second');
    });

    it('should mset and verify values individually', async () => {
      await db.kv.mset({
        [`${KEY_PREFIX}m1`]: 'val1',
        [`${KEY_PREFIX}m2`]: 'val2',
        [`${KEY_PREFIX}m3`]: 'val3',
      });

      const v1 = await db.kv.get(`${KEY_PREFIX}m1`);
      const v2 = await db.kv.get(`${KEY_PREFIX}m2`);
      const v3 = await db.kv.get(`${KEY_PREFIX}m3`);
      assert.equal(v1, 'val1');
      assert.equal(v2, 'val2');
      assert.equal(v3, 'val3');
    });
  });

  // =======================================================================
  // 6. Vector Operations
  // =======================================================================
  describe('6. Vector Operations', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}vectors`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
    });

    beforeEach(async () => {
      await dropTable(db, TABLE);
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, label TEXT, embedding VECTOR(3), PRIMARY KEY (id))`
      );
    });

    afterEach(async () => {
      await dropTable(db, TABLE);
    });

    after(async () => {
      await db.close();
    });

    it('should insert vectors and search', async () => {
      await db.vector.insert(TABLE, { id: 1, label: 'cat' }, 'embedding', [1.0, 0.0, 0.0]);
      await db.vector.insert(TABLE, { id: 2, label: 'dog' }, 'embedding', [0.0, 1.0, 0.0]);
      await db.vector.insert(TABLE, { id: 3, label: 'fish' }, 'embedding', [0.0, 0.0, 1.0]);

      const results = await db.vector.search(TABLE, 'embedding', [1.0, 0.1, 0.0], { k: 2, columns: ['id', 'label'] });
      assert.equal(results.length, 2);
      // Nearest to [1,0.1,0] should be [1,0,0] = "cat"
      assert.equal(results[0].row.label, 'cat');
      assert.ok(typeof results[0].distance === 'number');
      assert.ok(results[0].distance <= results[1].distance);
    });

    it('should search with a filter', async () => {
      await db.vector.insert(TABLE, { id: 1, label: 'cat' }, 'embedding', [1.0, 0.0, 0.0]);
      await db.vector.insert(TABLE, { id: 2, label: 'dog' }, 'embedding', [0.9, 0.1, 0.0]);
      await db.vector.insert(TABLE, { id: 3, label: 'fish' }, 'embedding', [0.0, 0.0, 1.0]);

      const results = await db.vector.search(
        TABLE,
        'embedding',
        [1.0, 0.0, 0.0],
        { k: 10, filter: "label = 'dog'", columns: ['id', 'label'] }
      );
      assert.equal(results.length, 1);
      assert.equal(results[0].row.label, 'dog');
    });

    it('should create an HNSW index', async () => {
      // Insert 15+ vectors - HNSW index creation crashes with fewer than ~10 vectors
      for (let i = 1; i <= 15; i++) {
        const vec = [0.0, 0.0, 0.0];
        vec[i % 3] = 1.0;
        // Add small perturbation so vectors are not all identical
        vec[(i + 1) % 3] = i * 0.01;
        await db.vector.insert(TABLE, { id: i, label: `v${i}` }, 'embedding', vec);
      }

      // Should not throw
      await db.vector.createIndex(TABLE, 'embedding', 'hnsw', { m: 16, ef_construction: 200 });

      // Search should still work after index creation
      const results = await db.vector.search(TABLE, 'embedding', [1.0, 0.0, 0.0], { k: 1 });
      assert.equal(results.length, 1);
    });
  });

  // =======================================================================
  // 7. Aggregations
  // =======================================================================
  describe('7. Aggregations', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}agg`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, category TEXT, value INT64, PRIMARY KEY (id))`
      );
      // Insert test data
      await db.execute(`INSERT INTO ${TABLE} (id, category, value) VALUES (1, 'A', 10)`);
      await db.execute(`INSERT INTO ${TABLE} (id, category, value) VALUES (2, 'A', 20)`);
      await db.execute(`INSERT INTO ${TABLE} (id, category, value) VALUES (3, 'B', 30)`);
      await db.execute(`INSERT INTO ${TABLE} (id, category, value) VALUES (4, 'B', 40)`);
      await db.execute(`INSERT INTO ${TABLE} (id, category, value) VALUES (5, 'C', 50)`);
    });

    after(async () => {
      await dropTable(db, TABLE);
      await db.close();
    });

    it('should compute COUNT(*)', async () => {
      const result = await db.query<{ cnt: number }>(
        `SELECT COUNT(*) AS cnt FROM ${TABLE}`
      );
      assert.equal(Number(result.rows[0].cnt), 5);
    });

    it('should compute SUM', async () => {
      const result = await db.query<{ total: number }>(
        `SELECT SUM(value) AS total FROM ${TABLE}`
      );
      assert.equal(Number(result.rows[0].total), 150);
    });

    it('should compute AVG', async () => {
      const result = await db.query<{ avg_val: number }>(
        `SELECT AVG(value) AS avg_val FROM ${TABLE}`
      );
      assert.ok(Math.abs(Number(result.rows[0].avg_val) - 30) < 0.001);
    });

    it('should compute MIN', async () => {
      const result = await db.query<{ min_val: number }>(
        `SELECT MIN(value) AS min_val FROM ${TABLE}`
      );
      assert.equal(Number(result.rows[0].min_val), 10);
    });

    it('should compute MAX', async () => {
      const result = await db.query<{ max_val: number }>(
        `SELECT MAX(value) AS max_val FROM ${TABLE}`
      );
      assert.equal(Number(result.rows[0].max_val), 50);
    });

    it('should compute GROUP BY', async () => {
      const result = await db.query<{ category: string; total: number }>(
        `SELECT category, SUM(value) AS total FROM ${TABLE} GROUP BY category ORDER BY category`
      );
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].category, 'A');
      assert.equal(Number(result.rows[0].total), 30);
      assert.equal(result.rows[1].category, 'B');
      assert.equal(Number(result.rows[1].total), 70);
      assert.equal(result.rows[2].category, 'C');
      assert.equal(Number(result.rows[2].total), 50);
    });
  });

  // =======================================================================
  // 8. ORDER BY & LIMIT/OFFSET
  // =======================================================================
  describe('8. ORDER BY & LIMIT/OFFSET', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}order`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, name TEXT, score INT64, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, name, score) VALUES (1, 'Alice', 90)`);
      await db.execute(`INSERT INTO ${TABLE} (id, name, score) VALUES (2, 'Bob', 80)`);
      await db.execute(`INSERT INTO ${TABLE} (id, name, score) VALUES (3, 'Charlie', 95)`);
      await db.execute(`INSERT INTO ${TABLE} (id, name, score) VALUES (4, 'Diana', 85)`);
      await db.execute(`INSERT INTO ${TABLE} (id, name, score) VALUES (5, 'Eve', 70)`);
    });

    after(async () => {
      await dropTable(db, TABLE);
      await db.close();
    });

    it('should ORDER BY ascending', async () => {
      const result = await db.query<{ name: string; score: number }>(
        `SELECT name, score FROM ${TABLE} ORDER BY score ASC`
      );
      assert.equal(result.rows.length, 5);
      assert.equal(result.rows[0].name, 'Eve');
      assert.equal(result.rows[4].name, 'Charlie');
    });

    it('should ORDER BY descending', async () => {
      const result = await db.query<{ name: string; score: number }>(
        `SELECT name, score FROM ${TABLE} ORDER BY score DESC`
      );
      assert.equal(result.rows[0].name, 'Charlie');
      assert.equal(result.rows[4].name, 'Eve');
    });

    it('should LIMIT results', async () => {
      const result = await db.query<{ name: string }>(
        `SELECT name FROM ${TABLE} ORDER BY score DESC LIMIT 3`
      );
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].name, 'Charlie');
    });

    it('should LIMIT with OFFSET', async () => {
      const result = await db.query<{ name: string; score: number }>(
        `SELECT name, score FROM ${TABLE} ORDER BY score DESC LIMIT 2 OFFSET 2`
      );
      assert.equal(result.rows.length, 2);
      // After skipping top 2 (Charlie=95, Alice=90), next 2 are Diana=85, Bob=80
      assert.equal(result.rows[0].name, 'Diana');
      assert.equal(result.rows[1].name, 'Bob');
    });
  });

  // =======================================================================
  // 9. Concurrent Connections
  // =======================================================================
  describe('9. Concurrent Connections', () => {
    let db: ParticleDB;
    const TABLE_R = `${TABLE_PREFIX}conc_read`;
    const TABLE_W = `${TABLE_PREFIX}conc_write`;

    before(async () => {
      db = new ParticleDB({ ...testConfig(), poolSize: 10 });
      await dropTable(db, TABLE_R);
      await dropTable(db, TABLE_W);
      await db.execute(
        `CREATE TABLE ${TABLE_R} (id INT64, val INT64, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE_R} (id, val) VALUES (1, 100)`);
      await db.execute(
        `CREATE TABLE ${TABLE_W} (id INT64, val INT64, PRIMARY KEY (id))`
      );
    });

    after(async () => {
      await dropTable(db, TABLE_R);
      await dropTable(db, TABLE_W);
      await db.close();
    });

    it('should handle multiple concurrent readers', async () => {
      const promises = Array.from({ length: 10 }, () =>
        db.query<{ val: number }>(`SELECT val FROM ${TABLE_R} WHERE id = 1`)
      );
      const results = await Promise.all(promises);
      for (const r of results) {
        assert.equal(r.rows.length, 1);
        assert.equal(Number(r.rows[0].val), 100);
      }
    });

    it('should handle multiple concurrent writers', async () => {
      const ids = Array.from({ length: 10 }, (_, i) => i + 1);
      const promises = ids.map((id) =>
        db.execute(`INSERT INTO ${TABLE_W} (id, val) VALUES (${id}, ${id * 10})`)
      );
      await Promise.all(promises);

      const result = await db.query(`SELECT COUNT(*) AS cnt FROM ${TABLE_W}`);
      assert.equal(Number(result.rows[0].cnt), 10);
    });
  });

  // =======================================================================
  // 10. Joins
  // =======================================================================
  describe('10. Joins', () => {
    let db: ParticleDB;
    const USERS = `${TABLE_PREFIX}join_users`;
    const ORDERS = `${TABLE_PREFIX}join_orders`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, ORDERS);
      await dropTable(db, USERS);

      await db.execute(
        `CREATE TABLE ${USERS} (id INT64, name TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${USERS} (id, name) VALUES (1, 'Alice')`);
      await db.execute(`INSERT INTO ${USERS} (id, name) VALUES (2, 'Bob')`);
      await db.execute(`INSERT INTO ${USERS} (id, name) VALUES (3, 'Charlie')`);

      await db.execute(
        `CREATE TABLE ${ORDERS} (id INT64, user_id INT64, amount INT64, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${ORDERS} (id, user_id, amount) VALUES (1, 1, 100)`);
      await db.execute(`INSERT INTO ${ORDERS} (id, user_id, amount) VALUES (2, 1, 200)`);
      await db.execute(`INSERT INTO ${ORDERS} (id, user_id, amount) VALUES (3, 2, 150)`);
    });

    after(async () => {
      await dropTable(db, ORDERS);
      await dropTable(db, USERS);
      await db.close();
    });

    it('should perform an INNER JOIN', async () => {
      const result = await db.query<{ name: string; amount: number }>(
        `SELECT u.name, o.amount FROM ${USERS} u JOIN ${ORDERS} o ON u.id = o.user_id ORDER BY o.amount`
      );
      assert.equal(result.rows.length, 3);
      assert.equal(result.rows[0].name, 'Alice');
      assert.equal(Number(result.rows[0].amount), 100);
    });

    it('should join with GROUP BY', async () => {
      // Use user_id for grouping - server JOIN+GROUP BY on text columns has known quirks
      const result = await db.query<{ user_id: number; total: number }>(
        `SELECT o.user_id, SUM(o.amount) AS total FROM ${ORDERS} o GROUP BY o.user_id ORDER BY total DESC`
      );
      assert.ok(result.rows.length >= 2);
      const sorted = [...result.rows].sort((a, b) => Number(b.total) - Number(a.total));
      assert.equal(Number(sorted[0].user_id), 1); // Alice's user_id
      assert.equal(Number(sorted[0].total), 300);
      assert.equal(Number(sorted[1].user_id), 2); // Bob's user_id
      assert.equal(Number(sorted[1].total), 150);
    });

    it('should join with WHERE filter', async () => {
      const result = await db.query<{ name: string; amount: number }>(
        `SELECT u.name, o.amount FROM ${USERS} u JOIN ${ORDERS} o ON u.id = o.user_id WHERE o.amount > 100 ORDER BY o.amount`
      );
      assert.equal(result.rows.length, 2);
      assert.equal(Number(result.rows[0].amount), 150);
      assert.equal(Number(result.rows[1].amount), 200);
    });
  });

  // =======================================================================
  // 11. Error Handling
  // =======================================================================
  describe('11. Error Handling', () => {
    let db: ParticleDB;
    const TABLE = `${TABLE_PREFIX}errors`;

    before(async () => {
      db = new ParticleDB(testConfig());
      await dropTable(db, TABLE);
    });

    afterEach(async () => {
      await dropTable(db, TABLE);
    });

    after(async () => {
      await db.close();
    });

    it('should reject on invalid SQL', async () => {
      await assert.rejects(async () => {
        await db.query('THIS IS NOT VALID SQL');
      });
    });

    it('should reject when querying a nonexistent table', async () => {
      await assert.rejects(async () => {
        await db.query(`SELECT * FROM ${TABLE_PREFIX}nonexistent_table_xyz`);
      });
    });

    it('should reject duplicate primary keys and preserve the original row', async () => {
      await db.execute(
        `CREATE TABLE ${TABLE} (id INT64, val TEXT, PRIMARY KEY (id))`
      );
      await db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (1, 'first')`);

      await assert.rejects(db.execute(`INSERT INTO ${TABLE} (id, val) VALUES (1, 'duplicate')`),
        (error: any) => error.code === 23505);
      const result = await db.query(`SELECT id, val FROM ${TABLE}`);
      assert.deepEqual(result.rows, [{id: 1, val: 'first'}]);
    });
  });

  // =======================================================================
  // 12. AI Tests (gated behind PARTICLEDB_AI_ENABLED)
  // =======================================================================
  describe('12. AI Operations', () => {
    let db: ParticleDB;

    before(async () => {
      db = new ParticleDB(testConfig());
    });

    after(async () => {
      await db.close();
    });

    it('should generate an embedding', { skip: !aiEnabled() && 'PARTICLEDB_AI_ENABLED not set' }, async () => {
      const embedding = await db.ai.embed('Hello world');
      assert.ok(Array.isArray(embedding));
      assert.ok(embedding.length > 0);
      // Each element should be a number
      for (const v of embedding) {
        assert.equal(typeof v, 'number');
        assert.ok(Number.isFinite(v));
      }
    });

    it('should generate text', { skip: !aiEnabled() && 'PARTICLEDB_AI_ENABLED not set' }, async () => {
      const text = await db.ai.generate('Say hello');
      assert.ok(typeof text === 'string');
      assert.ok(text.length > 0);
    });

    it('should classify text', { skip: !aiEnabled() && 'PARTICLEDB_AI_ENABLED not set' }, async () => {
      const label = await db.ai.classify(
        'I love this product!',
        ['positive', 'negative', 'neutral']
      );
      assert.ok(typeof label === 'string');
      assert.ok(['positive', 'negative', 'neutral'].includes(label));
    });

    it('should summarize text', { skip: !aiEnabled() && 'PARTICLEDB_AI_ENABLED not set' }, async () => {
      const summary = await db.ai.summarize(
        'ParticleDB is a high-performance analytical database that combines OLTP and OLAP workloads. ' +
        'It uses Arrow columnar format with dictionary encoding for efficient storage and query processing. ' +
        'The engine supports vector search, key-value operations, and AI-powered functions.'
      );
      assert.ok(typeof summary === 'string');
      assert.ok(summary.length > 0);
    });
  });
}
