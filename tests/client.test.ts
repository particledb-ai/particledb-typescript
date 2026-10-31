import { describe, it, beforeEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import { inspect } from 'node:util';

// ---------------------------------------------------------------------------
// Mock pg.Pool
// ---------------------------------------------------------------------------

/** Creates a mock Pool that records all query calls and returns configurable results. */
function createMockPool(queryResult?: any) {
  const defaultResult = queryResult ?? { rows: [], rowCount: 0, fields: [] };

  const mockQuery = mock.fn(async (_sql: string, _params?: any[]) => defaultResult);

  const mockClientQuery = mock.fn(async (_sql: string, _params?: any[]) => defaultResult);
  const mockRelease = mock.fn();
  const mockConnect = mock.fn(async () => ({
    query: mockClientQuery,
    release: mockRelease,
  }));
  const mockEnd = mock.fn(async () => {});

  const pool = {
    query: mockQuery,
    connect: mockConnect,
    end: mockEnd,
  };

  return { pool, mockQuery, mockConnect, mockClientQuery, mockRelease, mockEnd };
}

import { ParticleDB as RealParticleDB } from '../src/client';
import { VectorOps } from '../src/vector';
import { KVOps } from '../src/kv';
import { RAGOps } from '../src/rag';
import { AIOps } from '../src/ai';

function client(pool: any): RealParticleDB {
  const db = new RealParticleDB();
  const convert = async (query: any, sql: string, params?: any[]) => {
    const r = await query(sql, params);
    return { rows: r.rows, rowCount: r.rowCount ?? 0, columns: r.fields?.map((f: any) => f.name) ?? [] };
  };
  (db as any).backend = {
    protocol: 'pdb', query: pool.query,
    queryResult: (sql: string, params?: any[]) => convert(pool.query, sql, params),
    transaction: async (fn: any) => {
      const c = await pool.connect();
      try {
        await c.query('BEGIN');
        const result = await fn({queryResult: (sql: string, params?: any[]) => convert(c.query, sql, params)});
        await c.query('COMMIT');
        return result;
      } catch (err) { await c.query('ROLLBACK'); throw err; }
      finally { c.release(); }
    },
    close: pool.end,
  };
  return db;
}

// ===========================================================================
// Tests
// ===========================================================================

describe('ParticleDB Client', () => {
  it('keeps credentials out of malformed connection URL errors', async () => {
    for (const dsn of ['pdb://app:private-credential@localhost:invalid', 'private-credential!']) {
      await assert.rejects(
        RealParticleDB.connect(dsn),
        (err: Error) => {
          assert.ok(!inspect(err).includes('private-credential'));
          assert.ok(!Object.hasOwn(err, 'input'));
          assert.equal((err as Error & { cause?: unknown }).cause, undefined);
          return true;
        },
      );
    }
  });

  // -----------------------------------------------------------------------
  // 1. Client creation with defaults
  // -----------------------------------------------------------------------
  it('should create a client with default configuration', () => {
    const { pool } = createMockPool();
    const db = client(pool);

    assert.ok(db);
    assert.equal(db.protocol, 'pdb');
  });

  // -----------------------------------------------------------------------
  // 2. Client creation with custom config
  // -----------------------------------------------------------------------
  it('should create a client with custom configuration', () => {
    const { pool } = createMockPool();
    const db = client(pool);

    // The pool is injected, so we verify the client is usable
    assert.ok(db);
    assert.equal(typeof db.query, 'function');
    assert.equal(typeof db.execute, 'function');
    assert.equal(typeof db.transaction, 'function');
    assert.equal(typeof db.close, 'function');
  });

  // -----------------------------------------------------------------------
  // 3. query returns typed results
  // -----------------------------------------------------------------------
  it('should return typed query results', async () => {
    const { pool } = createMockPool({
      rows: [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }],
      rowCount: 2,
      fields: [{ name: 'id' }, { name: 'name' }],
    });
    const db = client(pool);

    const result = await db.query<{ id: number; name: string }>(
      'SELECT id, name FROM users'
    );

    assert.equal(result.rows.length, 2);
    assert.equal(result.rowCount, 2);
    assert.deepEqual(result.columns, ['id', 'name']);
    assert.equal(result.rows[0].id, 1);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[1].id, 2);
    assert.equal(result.rows[1].name, 'Bob');
  });

  // -----------------------------------------------------------------------
  // 4. queryOne returns single row
  // -----------------------------------------------------------------------
  it('should return a single row with queryOne', async () => {
    const { pool } = createMockPool({
      rows: [{ id: 42, email: 'alice@example.com' }],
      rowCount: 1,
      fields: [{ name: 'id' }, { name: 'email' }],
    });
    const db = client(pool);

    const row = await db.queryOne<{ id: number; email: string }>(
      'SELECT * FROM users WHERE id = $1',
      [42]
    );

    assert.ok(row);
    assert.equal(row!.id, 42);
    assert.equal(row!.email, 'alice@example.com');
  });

  // -----------------------------------------------------------------------
  // 5. queryOne returns null for empty result
  // -----------------------------------------------------------------------
  it('should return null from queryOne when no rows match', async () => {
    const { pool } = createMockPool({
      rows: [],
      rowCount: 0,
      fields: [{ name: 'id' }],
    });
    const db = client(pool);

    const row = await db.queryOne('SELECT * FROM users WHERE id = $1', [999]);

    assert.equal(row, null);
  });

  // -----------------------------------------------------------------------
  // 6. execute returns row count
  // -----------------------------------------------------------------------
  it('should return affected row count from execute', async () => {
    const { pool } = createMockPool({
      rows: [],
      rowCount: 3,
      fields: [],
    });
    const db = client(pool);

    const count = await db.execute(
      'UPDATE users SET active = true WHERE created_at > $1',
      ['2024-01-01']
    );

    assert.equal(count, 3);
  });

  // -----------------------------------------------------------------------
  // 7. transaction commits on success
  // -----------------------------------------------------------------------
  it('should commit a transaction on success', async () => {
    const { pool, mockClientQuery, mockRelease } = createMockPool({
      rows: [],
      rowCount: 1,
      fields: [],
    });
    const db = client(pool);

    const result = await db.transaction(async (tx) => {
      await tx.execute('INSERT INTO users (name) VALUES ($1)', ['Alice']);
      return 'committed';
    });

    assert.equal(result, 'committed');

    // Verify BEGIN was called
    const calls = mockClientQuery.mock.calls;
    assert.equal(calls[0].arguments[0], 'BEGIN');

    // Verify the INSERT was called
    assert.equal(calls[1].arguments[0], 'INSERT INTO users (name) VALUES ($1)');
    assert.deepEqual(calls[1].arguments[1], ['Alice']);

    // Verify COMMIT was called
    assert.equal(calls[2].arguments[0], 'COMMIT');

    // Verify client was released
    assert.equal(mockRelease.mock.callCount(), 1);
  });

  // -----------------------------------------------------------------------
  // 8. transaction rolls back on error
  // -----------------------------------------------------------------------
  it('should rollback a transaction on error', async () => {
    const { pool, mockClientQuery, mockRelease } = createMockPool({
      rows: [],
      rowCount: 0,
      fields: [],
    });
    const db = client(pool);

    // Make the second client query (the user's INSERT) throw
    let callCount = 0;
    mockClientQuery.mock.mockImplementation(async (sql: string) => {
      callCount++;
      if (callCount === 2) {
        throw new Error('constraint violation');
      }
      return { rows: [], rowCount: 0, fields: [] };
    });

    await assert.rejects(
      () =>
        db.transaction(async (tx) => {
          await tx.execute('INSERT INTO users (id) VALUES ($1)', [1]);
        }),
      { message: 'constraint violation' }
    );

    // Verify ROLLBACK was called (call 3: BEGIN, INSERT throws, ROLLBACK)
    const calls = mockClientQuery.mock.calls;
    const lastSql = calls[calls.length - 1].arguments[0];
    assert.equal(lastSql, 'ROLLBACK');

    // Verify client was released
    assert.equal(mockRelease.mock.callCount(), 1);
  });

  // -----------------------------------------------------------------------
  // 9. vector.search generates correct SQL
  // -----------------------------------------------------------------------
  it('should generate correct vector search SQL', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ id: 1, title: 'doc1', __distance: 0.1 }],
      rowCount: 1,
      fields: [],
    });
    const db = client(pool);

    const results = await db.vector.search('documents', 'embedding', [0.1, 0.2, 0.3], {
      k: 5,
      filter: 'category = \'science\'',
    });

    const sql = mockQuery.mock.calls[0].arguments[0] as string;
    assert.ok(sql.includes('(embedding <-> $1)'));
    assert.ok(sql.includes('FROM documents'));
    assert.ok(sql.includes("WHERE category = 'science'"));
    assert.ok(sql.includes('LIMIT $2'));
    assert.deepEqual(mockQuery.mock.calls[0].arguments[1], ['[0.1,0.2,0.3]', 5]);

    assert.equal(results.length, 1);
    assert.equal(results[0].distance, 0.1);
    assert.equal(results[0].row.id, 1);
    assert.equal(results[0].row.title, 'doc1');
    // __distance should be removed from the row
    assert.equal(results[0].row.__distance, undefined);
  });

  // -----------------------------------------------------------------------
  // 10. kv.set generates correct SQL
  // -----------------------------------------------------------------------
  it('should generate correct KV SET SQL', async () => {
    const { pool, mockQuery } = createMockPool();
    const db = client(pool);

    await db.kv.set('mykey', 'myvalue', 3600);

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT KV_SET($1, $2, $3)');
    assert.deepEqual(call.arguments[1], ['mykey', 'myvalue', 3600]);
  });

  // -----------------------------------------------------------------------
  // 11. kv.get generates correct SQL
  // -----------------------------------------------------------------------
  it('should generate correct KV GET SQL', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ value: 'hello' }],
      rowCount: 1,
      fields: [{ name: 'value' }],
    });
    const db = client(pool);

    const val = await db.kv.get('mykey');

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT KV_GET($1) AS value');
    assert.deepEqual(call.arguments[1], ['mykey']);
    assert.equal(val, 'hello');
  });

  // -----------------------------------------------------------------------
  // 12. kv.mset with multiple keys
  // -----------------------------------------------------------------------
  it('should generate correct KV MSET SQL for multiple keys', async () => {
    const { pool, mockQuery } = createMockPool();
    const db = client(pool);

    await db.kv.mset({ key1: 'val1', key2: 'val2', key3: 'val3' });

    const call = mockQuery.mock.calls[0];
    const sql = call.arguments[0] as string;
    assert.ok(sql.includes('KV_MSET'));
    assert.ok(sql.includes('$1'));
    assert.ok(sql.includes('$6')); // 3 keys * 2 = 6 params
    assert.deepEqual(call.arguments[1], ['key1', 'val1', 'key2', 'val2', 'key3', 'val3']);
  });

  // -----------------------------------------------------------------------
  // 13. rag.query SQL generation
  // -----------------------------------------------------------------------
  it('should generate correct RAG query SQL with pipeline', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ answer: 'ParticleDB is an analytical database.' }],
      rowCount: 1,
      fields: [{ name: 'answer' }],
    });
    const db = client(pool);

    const answer = await db.rag.query('What is ParticleDB?', {
      pipeline: 'docs_pipeline',
    });

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT RAG($1, $2) AS answer');
    assert.deepEqual(call.arguments[1], ['What is ParticleDB?', 'docs_pipeline']);
    assert.equal(answer, 'ParticleDB is an analytical database.');
  });

  // -----------------------------------------------------------------------
  // 14. ai.embed SQL generation
  // -----------------------------------------------------------------------
  it('should generate correct AI embed SQL', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ embedding: [0.1, 0.2, 0.3] }],
      rowCount: 1,
      fields: [{ name: 'embedding' }],
    });
    const db = client(pool);

    const embedding = await db.ai.embed('Hello world', 'text-embedding-3-small');

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT AI_EMBED($1, $2) AS embedding');
    assert.deepEqual(call.arguments[1], ['Hello world', 'text-embedding-3-small']);
    assert.deepEqual(embedding, [0.1, 0.2, 0.3]);
  });

  // -----------------------------------------------------------------------
  // 15. close releases pool
  // -----------------------------------------------------------------------
  it('should release the pool on close', async () => {
    const { pool, mockEnd } = createMockPool();
    const db = client(pool);

    await db.close();

    assert.equal(mockEnd.mock.callCount(), 1);
  });

  // -----------------------------------------------------------------------
  // 16. kv.set without TTL
  // -----------------------------------------------------------------------
  it('should generate KV SET SQL without TTL', async () => {
    const { pool, mockQuery } = createMockPool();
    const db = client(pool);

    await db.kv.set('key', 'value');

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT KV_SET($1, $2)');
    assert.deepEqual(call.arguments[1], ['key', 'value']);
  });

  // -----------------------------------------------------------------------
  // 17. kv.get returns null for missing key
  // -----------------------------------------------------------------------
  it('should return null from kv.get for missing key', async () => {
    const { pool } = createMockPool({
      rows: [{ value: null }],
      rowCount: 1,
      fields: [{ name: 'value' }],
    });
    const db = client(pool);

    const val = await db.kv.get('nonexistent');
    assert.equal(val, null);
  });

  // -----------------------------------------------------------------------
  // 18. vector.search with default k
  // -----------------------------------------------------------------------
  it('should use default k=10 for vector search', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [],
      rowCount: 0,
      fields: [],
    });
    const db = client(pool);

    await db.vector.search('docs', 'vec', [1.0, 2.0]);

    const sql = mockQuery.mock.calls[0].arguments[0] as string;
    assert.ok(sql.includes('LIMIT $2'));
    assert.deepEqual(mockQuery.mock.calls[0].arguments[1], ['[1,2]', 10]);
  });

  // -----------------------------------------------------------------------
  // 19. ai.embed without model parameter
  // -----------------------------------------------------------------------
  it('should generate AI embed SQL without model', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ embedding: [0.5] }],
      rowCount: 1,
      fields: [{ name: 'embedding' }],
    });
    const db = client(pool);

    await db.ai.embed('test');

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT AI_EMBED($1) AS embedding');
    assert.deepEqual(call.arguments[1], ['test']);
  });

  // -----------------------------------------------------------------------
  // 20. rag.query without pipeline (inline config)
  // -----------------------------------------------------------------------
  it('should generate RAG query SQL with inline table config', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ answer: 'An answer.' }],
      rowCount: 1,
      fields: [{ name: 'answer' }],
    });
    const db = client(pool);

    await db.rag.query('question', {
      table: 'my_docs',
      vectorCol: 'emb',
      textCol: 'body',
      k: 3,
    });

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT RAG_QUERY($1, $2, $3, $4, $5) AS answer');
    assert.deepEqual(call.arguments[1], ['question', 'my_docs', 'emb', 'body', 3]);
  });

  // -----------------------------------------------------------------------
  // 21. vector.createIndex generates correct DDL
  // -----------------------------------------------------------------------
  it('should generate correct vector createIndex SQL', async () => {
    const { pool, mockQuery } = createMockPool();
    const db = client(pool);

    await db.vector.createIndex('docs', 'embedding', 'hnsw', {
      m: 16,
      ef_construction: 200,
    });

    const sql = mockQuery.mock.calls[0].arguments[0] as string;
    assert.ok(sql.includes('CREATE INDEX idx_docs_embedding_hnsw'));
    assert.ok(sql.includes('USING hnsw'));
    assert.ok(sql.includes('(embedding)'));
    assert.ok(sql.includes('WITH (m = 16, ef_construction = 200)'));
  });

  // -----------------------------------------------------------------------
  // 22. ai.classify generates correct SQL
  // -----------------------------------------------------------------------
  it('should generate correct AI classify SQL', async () => {
    const { pool, mockQuery } = createMockPool({
      rows: [{ label: 'positive' }],
      rowCount: 1,
      fields: [{ name: 'label' }],
    });
    const db = client(pool);

    const label = await db.ai.classify('Great product!', ['positive', 'negative']);

    const call = mockQuery.mock.calls[0];
    assert.equal(call.arguments[0], 'SELECT AI_CLASSIFY($1, $2) AS label');
    assert.equal(call.arguments[1]![0], 'Great product!');
    assert.equal(call.arguments[1]![1], '["positive","negative"]');
    assert.equal(label, 'positive');
  });
});
