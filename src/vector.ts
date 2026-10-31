import type { Queryable } from './types';
import { VectorSearchResult, VectorSearchOptions } from './types';
import { assertIdentifier, escapeLiteral } from './sql';

const IDENT_MESSAGE = 'vector tables, columns and option names must be simple SQL identifiers';

/**
 * Encode a vector as the `[x,y,...]` text the server parses, refusing anything
 * that is not a non-empty array of finite numbers (the TypeScript type is not
 * enforced at runtime).
 */
function vectorText(vector: unknown): string {
  if (!Array.isArray(vector) || vector.length === 0) {
    throw new TypeError('vector must be a non-empty array of finite numbers');
  }
  for (const x of vector) {
    if (typeof x !== 'number' || !Number.isFinite(x)) {
      throw new TypeError('vector must be a non-empty array of finite numbers');
    }
  }
  return `[${vector.join(',')}]`;
}

/**
 * Vector search operations for ParticleDB.
 *
 * Provides methods for similarity search, vector insertion,
 * and index management on vector columns.
 *
 * Table and column names (and `data` keys / index option names) must be
 * unquoted SQL identifiers (`schema.table` allowed for tables); vectors must
 * be non-empty arrays of finite numbers and `k` a positive integer. Anything
 * else throws `TypeError` / `RangeError` before a statement is sent.
 */
export class VectorOps {
  constructor(private pool: Queryable) {}

  /**
   * Perform a vector similarity search on a table.
   *
   * @param table - Table name containing the vectors.
   * @param column - Column name storing the vector data.
   * @param queryVector - The query vector to find nearest neighbors for.
   * @param options - Optional search parameters (filter, k).
   * @returns Array of search results sorted by distance (ascending).
   *
   * @example
   * ```ts
   * const results = await db.vector.search('documents', 'embedding',
   *   [0.1, 0.2, 0.3], { k: 5 });
   * ```
   */
  async search(
    table: string,
    column: string,
    queryVector: number[],
    options?: VectorSearchOptions
  ): Promise<VectorSearchResult[]> {
    assertIdentifier(table, true, IDENT_MESSAGE);
    assertIdentifier(column, false, IDENT_MESSAGE);
    const vector = vectorText(queryVector);
    const k = options?.k === undefined ? 10 : options.k;
    if (typeof k !== 'number' || !Number.isSafeInteger(k) || k <= 0) {
      throw new RangeError('k must be a positive integer');
    }

    // L2 distance via the `<->` operator (the server has no VECTOR_DISTANCE
    // function; `<->` is what an HNSW index accelerates). The vector and k are
    // bound parameters; `filter` is caller-written SQL by contract.
    let sql = `SELECT *, (${column} <-> $1) AS __distance FROM ${table}`;

    if (options?.filter) {
      sql += ` WHERE ${options.filter}`;
    }

    sql += ` ORDER BY __distance LIMIT $2`;

    const result = await this.pool.query(sql, [vector, k]);

    return result.rows.map((row: Record<string, any>) => {
      const distance = row.__distance;
      const { __distance, ...rest } = row;
      return { row: rest, distance };
    });
  }

  /**
   * Insert a row with a vector value into a table.
   *
   * @param table - Target table name.
   * @param data - Non-vector column values as key-value pairs.
   * @param vectorColumn - Name of the vector column.
   * @param vector - The vector value to insert.
   *
   * @example
   * ```ts
   * await db.vector.insert('documents', { title: 'My Doc' },
   *   'embedding', [0.1, 0.2, 0.3]);
   * ```
   */
  async insert(
    table: string,
    data: Record<string, any>,
    vectorColumn: string,
    vector: number[]
  ): Promise<void> {
    assertIdentifier(table, true, IDENT_MESSAGE);
    const allData = { ...data, [vectorColumn]: vectorText(vector) };
    const columns = Object.keys(allData);
    for (const c of columns) assertIdentifier(c, false, IDENT_MESSAGE);
    const values = Object.values(allData);
    const placeholders = columns.map((_, i) => `$${i + 1}`);

    const sql = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`;
    await this.pool.query(sql, values);
  }

  /**
   * Create a vector index on a column.
   *
   * @param table - Table name.
   * @param column - Vector column name.
   * @param indexType - Index algorithm: 'hnsw' or 'ivfflat'. Default: 'hnsw'.
   * @param params - Additional index parameters (e.g., { m: 16, ef_construction: 200 }).
   *
   * @example
   * ```ts
   * await db.vector.createIndex('documents', 'embedding', 'hnsw',
   *   { m: 16, ef_construction: 200 });
   * ```
   */
  async createIndex(
    table: string,
    column: string,
    indexType: 'hnsw' | 'ivfflat' = 'hnsw',
    params?: Record<string, any>
  ): Promise<void> {
    assertIdentifier(table, true, IDENT_MESSAGE);
    assertIdentifier(column, false, IDENT_MESSAGE);
    if (indexType !== 'hnsw' && indexType !== 'ivfflat') {
      throw new TypeError("indexType must be 'hnsw' or 'ivfflat'");
    }
    const indexName = `idx_${table.replace(/\./g, '_')}_${column}_${indexType}`;
    let sql = `CREATE INDEX ${indexName} ON ${table} USING ${indexType} (${column})`;

    if (params && Object.keys(params).length > 0) {
      // DDL options take no bound parameters: names must be identifiers,
      // numbers must be finite, strings go through the one literal encoder.
      const withClause = Object.entries(params)
        .map(([k, v]) => {
          assertIdentifier(k, false, IDENT_MESSAGE);
          if ((typeof v === 'number' && Number.isFinite(v)) || typeof v === 'boolean') return `${k} = ${v}`;
          if (typeof v === 'string') return `${k} = '${escapeLiteral(v)}'`;
          throw new TypeError('index options must be finite numbers, booleans or strings');
        })
        .join(', ');
      sql += ` WITH (${withClause})`;
    }

    await this.pool.query(sql);
  }
}
