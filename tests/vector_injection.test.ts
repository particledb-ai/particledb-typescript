/**
 * SQL-injection regressions for `VectorOps` (src/vector.ts), run against a
 * recording pool (no server). Every payload must either be refused before a
 * statement is sent or travel as a bound parameter / a correctly escaped
 * literal -- never as SQL text.
 *
 *   node --import tsx --test tests/vector_injection.test.ts
 */
import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';

import { VectorOps } from '../src/vector';

const PAYLOADS = [
  "'); DROP TABLE t; --",
  "\\'",
  "1]'); DROP TABLE t; --",
  '{"a":"it\'s \\\\ \\"q\\""}',
  'é✓ 漢字 \u{1F600}',
];

function recorder() {
  const query = mock.fn(async (_sql: string, _params?: any[]) => ({ rows: [], rowCount: 0, fields: [] }));
  return { ops: new VectorOps({ query } as any), query };
}

/**
 * Remove every standard-conforming string literal ('...' with '' as the only
 * escape) and return what is left. Anything left over is SQL text the server
 * would parse as code; an unbalanced quote also shows up here.
 */
function codeOutsideLiterals(sql: string): string {
  return sql.replace(/'(?:[^']|'')*'/g, '<lit>');
}

describe('VectorOps.search refuses or binds hostile input', () => {
  it('binds the query vector and k as parameters', async () => {
    const { ops, query } = recorder();
    await ops.search('docs', 'embedding', [0.1, -2, 3e-7], { k: 5 });
    const [sql, params] = query.mock.calls[0].arguments;
    assert.equal(sql, 'SELECT *, (embedding <-> $1) AS __distance FROM docs ORDER BY __distance LIMIT $2');
    assert.deepEqual(params, ['[0.1,-2,3e-7]', 5]);
  });

  for (const p of PAYLOADS) {
    it(`refuses a non-number vector element ${JSON.stringify(p)}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.search('docs', 'embedding', [p as any]), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }

  for (const bad of [[[1, "'"]], [NaN], [Infinity], [1, null], ['1'], [], 'abc', { length: 1, 0: 1 }]) {
    it(`refuses vector ${JSON.stringify(bad)}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.search('docs', 'embedding', bad as any), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }

  for (const k of ['1; DROP TABLE t', "'); DROP TABLE t; --", 0, -1, 1.5, NaN, Infinity, 2 ** 53, '5', null]) {
    it(`refuses k = ${JSON.stringify(k)}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.search('docs', 'embedding', [1, 2], { k: k as any }), RangeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }

  for (const [table, column] of [["docs; DROP TABLE t; --", 'embedding'], ['docs', "embedding) FROM t; DROP TABLE t; --"], ['"docs"', 'e'], ['docs.', 'e'], ['', 'e']]) {
    it(`refuses identifiers ${JSON.stringify([table, column])}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.search(table, column, [1, 2]), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }

  it('accepts a schema-qualified table', async () => {
    const { ops, query } = recorder();
    await ops.search('public.docs', 'embedding', [1]);
    assert.match(query.mock.calls[0].arguments[0], /FROM public\.docs /);
  });
});

describe('VectorOps.insert refuses or binds hostile input', () => {
  it('binds every data value and the vector', async () => {
    const { ops, query } = recorder();
    await ops.insert('docs', { title: PAYLOADS[0], body: PAYLOADS[3], u: PAYLOADS[4] }, 'embedding', [1, 2.5]);
    const [sql, params] = query.mock.calls[0].arguments;
    assert.equal(sql, 'INSERT INTO docs (title, body, u, embedding) VALUES ($1, $2, $3, $4)');
    assert.deepEqual(params, [PAYLOADS[0], PAYLOADS[3], PAYLOADS[4], '[1,2.5]']);
  });

  for (const bad of [["1]'); DROP TABLE t; --"], [[1, "'"]], [NaN], []]) {
    it(`refuses vector ${JSON.stringify(bad)}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.insert('docs', {}, 'embedding', bad as any), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }

  for (const [table, data, col] of [
    ['docs (id) VALUES (1); DROP TABLE t; --', {}, 'e'],
    ['docs', { "title) VALUES ('x'); DROP TABLE t; --": 1 }, 'e'],
    ['docs', {}, "e) VALUES ('[1]'); DROP TABLE t; --"],
  ] as const) {
    it(`refuses identifiers ${JSON.stringify([table, Object.keys(data), col])}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.insert(table, data, col, [1]), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }
});

describe('VectorOps.createIndex escapes WITH values and refuses hostile names', () => {
  for (const p of PAYLOADS) {
    it(`keeps string option ${JSON.stringify(p)} inside one literal`, async () => {
      const { ops, query } = recorder();
      await ops.createIndex('docs', 'embedding', 'hnsw', { m: p });
      const sql = query.mock.calls[0].arguments[0] as string;
      assert.equal(codeOutsideLiterals(sql), 'CREATE INDEX idx_docs_embedding_hnsw ON docs USING hnsw (embedding) WITH (m = <lit>)');
      assert.ok(sql.endsWith(`WITH (m = '${p.replace(/'/g, "''")}')`));
    });
  }

  it('keeps numeric options unquoted', async () => {
    const { ops, query } = recorder();
    await ops.createIndex('docs', 'embedding', 'hnsw', { m: 16, ef_construction: 200 });
    assert.ok((query.mock.calls[0].arguments[0] as string).endsWith('WITH (m = 16, ef_construction = 200)'));
  });

  for (const params of [{ "m = 1); DROP TABLE t; --": 1 }, { m: NaN }, { m: Infinity }, { m: { toString: () => "1); DROP TABLE t; --" } }, { m: [1, "'"] }, { m: null }]) {
    it(`refuses options ${JSON.stringify(Object.keys(params))}=${String(Object.values(params)[0])}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.createIndex('docs', 'embedding', 'hnsw', params as any), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }

  for (const [table, column, type] of [
    ['docs ON t; DROP TABLE t; --', 'e', 'hnsw'],
    ['docs', 'e); DROP TABLE t; --', 'hnsw'],
    ['docs', 'e', 'hnsw (e); DROP TABLE t; --'],
  ]) {
    it(`refuses ${JSON.stringify([table, column, type])}`, async () => {
      const { ops, query } = recorder();
      await assert.rejects(ops.createIndex(table, column, type as any), TypeError);
      assert.equal(query.mock.callCount(), 0);
    });
  }
});
