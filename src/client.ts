import { ParticleDBConfig, QueryResult, Queryable } from './types';
import {
  PdbWireClient,
  PdbError,
  ConnectionError,
  BatchExecuteError,
  BatchExecuteResult,
  BatchRowError,
  TransactionContext,
  RawQueryResult,
  ReservedConnection,
} from './pdb_wire';
import type { ParamType, DecodeOptions } from './arrow_ipc';
import { VectorOps } from './vector';
import { KVOps } from './kv';
import { RAGOps } from './rag';
import { AIOps } from './ai';
import { ExtendedOps } from './ops';

/** Default PDB native wire port. */
const DEFAULT_NATIVE_PORT = 7173;
/** Default PostgreSQL wire port (only with `protocol: 'pg'`). */
const DEFAULT_PG_PORT = 5432;

/** Options for `executeBatch`. */
export interface ExecuteBatchOptions {
  /** Explicit parameter column types; inferred from the values when omitted. */
  types?: ParamType[];
}

// ---------------------------------------------------------------------------
// Backends: the native wire (default) and the optional PG wire.
// ---------------------------------------------------------------------------

/** Statement execution inside one transaction. @internal */
interface TxBackend {
  queryResult<T>(sql: string, params?: any[]): Promise<QueryResult<T>>;
  executeBatch(sql: string, rows: any[][], opts?: ExecuteBatchOptions): Promise<BatchExecuteResult>;
}

/** One wire protocol behind `ParticleDB`. @internal */
interface Backend extends Queryable, TxBackend {
  readonly protocol: 'pdb' | 'pg';
  transaction<T>(fn: (tx: TxBackend) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

/** PDB native wire backend (default). @internal */
class NativeBackend implements Backend {
  readonly protocol = 'pdb' as const;
  constructor(readonly client: PdbWireClient) {}

  async query(sql: string, params?: any[]) {
    const r = await this.client.queryResult(sql, params);
    return { rows: r.rows, rowCount: r.rowCount, fields: r.columns.map((name) => ({ name })) };
  }
  queryResult<T>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    return this.client.queryResult<T>(sql, params);
  }
  executeBatch(sql: string, rows: any[][], opts?: ExecuteBatchOptions): Promise<BatchExecuteResult> {
    return this.client.executeBatch(sql, rows, opts);
  }
  transaction<T>(fn: (tx: TxBackend) => Promise<T>): Promise<T> {
    return this.client.transaction((ctx: TransactionContext) =>
      fn({
        queryResult: <R>(sql: string, params?: any[]) => ctx.queryResult<R>(sql, params),
        executeBatch: (sql, rows, opts) => ctx.executeBatch(sql, rows, opts),
      }),
    );
  }
  close(): Promise<void> {
    return this.client.close();
  }
}

/** Load the optional `pg` package, or explain how to get it. */
/** `?transport=` on a `pdb://` URL: auto | unix | tcp (anything else is an error). */
function parseTransportParam(v: string | null): 'auto' | 'unix' | 'tcp' | undefined {
  if (v === null) return undefined;
  if (v === 'auto' || v === 'unix' || v === 'tcp') return v;
  throw new PdbError(`invalid transport '${v}' in connection string (expected auto, unix or tcp)`);
}

function loadPg(): any {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require('pg');
  } catch {
    throw new PdbError(
      "protocol 'pg' needs the optional 'pg' package: run `npm install pg` " +
        "(or use the default native protocol, port 7173)",
    );
  }
}

function pgResult<T>(result: any): QueryResult<T> {
  return {
    rows: result.rows as T[],
    rowCount: result.rowCount ?? 0,
    columns: result.fields ? result.fields.map((f: { name: string }) => f.name) : [],
  };
}

/** Run a parameterized statement once per row over a pg client. */
async function pgExecuteBatch(
  q: (sql: string, params: any[]) => Promise<any>,
  sql: string,
  rows: any[][],
  stopOnError: boolean,
): Promise<BatchExecuteResult> {
  const rowCounts: Array<number | null> = new Array(rows.length).fill(null);
  const errors: BatchRowError[] = [];
  for (let i = 0; i < rows.length; i++) {
    try {
      rowCounts[i] = (await q(sql, rows[i])).rowCount ?? 0;
    } catch (err) {
      errors.push({ row: i, message: (err as Error).message });
      if (stopOnError) break;
    }
  }
  if (errors.length > 0) throw new BatchExecuteError(rowCounts, errors);
  const counts = rowCounts as number[];
  return { rowCounts: counts, totalRowCount: counts.reduce((a, b) => a + b, 0) };
}

/** PostgreSQL wire backend over a `pg` Pool (explicit opt-in). @internal */
class PgBackend implements Backend {
  readonly protocol = 'pg' as const;
  constructor(readonly pool: any) {}

  query(sql: string, params?: any[]) {
    return this.pool.query(sql, params);
  }
  async queryResult<T>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    return pgResult<T>(await this.pool.query(sql, params));
  }
  executeBatch(sql: string, rows: any[][]): Promise<BatchExecuteResult> {
    // pg has no batch frame: one autocommit statement per row.
    return pgExecuteBatch((s, p) => this.pool.query(s, p), sql, rows, false);
  }
  async transaction<T>(fn: (tx: TxBackend) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn({
        queryResult: async <R>(sql: string, params?: any[]) => pgResult<R>(await client.query(sql, params)),
        executeBatch: (sql, rows) => pgExecuteBatch((s, p) => client.query(s, p), sql, rows, true),
      });
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
  }
  close(): Promise<void> {
    return this.pool.end();
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * A transaction context for executing multiple queries atomically.
 *
 * Obtained via `ParticleDB.transaction()`. All queries executed through
 * a Transaction instance run within a single database transaction on one
 * connection.
 */
export class Transaction {
  /** @internal */
  constructor(private tx: TxBackend) {}

  /**
   * Execute a SQL query within this transaction.
   *
   * @param sql - The SQL statement (may contain $1, $2, ... placeholders).
   * @param params - Optional parameter values for the placeholders.
   * @returns The query result with typed rows.
   */
  async query<T = Record<string, any>>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    return this.tx.queryResult<T>(sql, params);
  }

  /**
   * Execute a SQL statement that modifies data (INSERT, UPDATE, DELETE).
   *
   * @param sql - The SQL statement.
   * @param params - Optional parameter values.
   * @returns The number of rows affected.
   */
  async execute(sql: string, params?: any[]): Promise<number> {
    return (await this.tx.queryResult(sql, params)).rowCount;
  }

  /**
   * Run one parameterized statement once per parameter set inside this
   * transaction (native: a single MSG_EXECUTE_BATCH). The first failing set
   * rejects with `BatchExecuteError` and the transaction is rolled back.
   */
  async executeBatch(sql: string, rows: any[][], options?: ExecuteBatchOptions): Promise<BatchExecuteResult> {
    return this.tx.executeBatch(sql, rows, options);
  }
}

/**
 * The main ParticleDB client.
 *
 * Talks to the server over the PDB native wire (port 7173) by default --
 * binary frames, Arrow IPC results, native prepared statements and
 * EXECUTE_BATCH. The PostgreSQL wire is available as an explicit opt-in
 * (`protocol: 'pg'` or a `postgresql://` connection string) and needs the
 * optional `pg` package.
 *
 * @example
 * ```ts
 * import { ParticleDB } from '@particledb/sdk';
 *
 * const db = new ParticleDB({ host: 'localhost' }); // native wire, port 7173
 *
 * const result = await db.query<{ id: number; name: string }>(
 *   'SELECT id, name FROM users WHERE id = $1', [1]
 * );
 *
 * await db.close();
 * ```
 */
export class ParticleDB {
  private backend: Backend;
  private _vector: VectorOps | null = null;
  private _kv: KVOps | null = null;
  private _rag: RAGOps | null = null;
  private _ai: AIOps | null = null;
  private _ops: ExtendedOps | null = null;

  /**
   * Create a new ParticleDB client with the given configuration.
   *
   * @param config - Connection configuration. All fields are optional
   *                 and fall back to sensible defaults.
   */
  constructor(config?: ParticleDBConfig) {
    const protocol = config?.protocol ?? 'pdb';
    if (protocol === 'pg') {
      const pg = loadPg();
      this.backend = new PgBackend(
        new pg.Pool({
          host: config?.host ?? 'localhost',
          port: config?.port ?? DEFAULT_PG_PORT,
          user: config?.user ?? 'particledb',
          password: config?.password,
          database: config?.database ?? 'particledb',
          max: config?.poolSize ?? 10,
          ssl: config?.ssl || undefined,
        }),
      );
    } else if (protocol === 'pdb') {
      this.backend = new NativeBackend(
        new PdbWireClient({
          host: config?.host ?? 'localhost',
          port: config?.port ?? DEFAULT_NATIVE_PORT,
          poolSize: config?.poolSize ?? 10,
          user: config?.user,
          password: config?.password,
          timeoutMs: config?.timeoutMs,
          ssl: config?.ssl,
          transport: config?.transport,
          unixSocketDir: config?.unixSocketDir,
        }),
      );
    } else {
      throw new PdbError(`unknown protocol '${String(protocol)}' (expected 'pdb' or 'pg')`);
    }
  }

  /**
   * Create a ParticleDB client from a connection string and verify connectivity.
   *
   * - `pdb://[user[:password]@]host[:port]` (or `particledb://`) uses the
   *   native wire (default port 7173).
   * - `postgresql://...` / `postgres://...` explicitly selects the PG wire
   *   (default port 5432) and needs the optional `pg` package.
   *
   * @example
   * ```ts
   * const db = await ParticleDB.connect('pdb://localhost:7173');
   * ```
   */
  static async connect(connectionString: string | ParticleDBConfig = {}): Promise<ParticleDB> {
    if (typeof connectionString !== 'string') {
      const db = new ParticleDB(connectionString);
      try { await db.query('SELECT 1'); return db; }
      catch (err) { await db.close(); throw err; }
    }
    const scheme = connectionString.slice(0, connectionString.indexOf(':')).toLowerCase();
    let db: ParticleDB;
    if (scheme === 'postgres' || scheme === 'postgresql') {
      const pg = loadPg();
      db = Object.create(ParticleDB.prototype) as ParticleDB;
      ParticleDB.initFields(db, new PgBackend(new pg.Pool({ connectionString })));
    } else if (scheme === 'pdb' || scheme === 'particledb') {
      let url: URL;
      try { url = new URL(connectionString); }
      catch { throw new PdbError('Invalid native connection URL'); }
      for (const key of url.searchParams.keys()) {
        if (!['transport', 'unixSocketDir', 'ssl'].includes(key)) throw new PdbError(`Unsupported connection option '${key}'`);
      }
      const ssl = url.searchParams.get('ssl');
      if (ssl !== null && ssl !== 'true' && ssl !== 'false') throw new PdbError('ssl must be true or false');
      db = new ParticleDB({
        protocol: 'pdb',
        ssl: url.searchParams.get('ssl') === 'true' || undefined,
        host: (url.hostname || 'localhost').replace(/^\[|\]$/g, ''),
        port: url.port ? Number(url.port) : DEFAULT_NATIVE_PORT,
        user: url.username ? decodeURIComponent(url.username) : undefined,
        password: url.password ? decodeURIComponent(url.password) : undefined,
        transport: parseTransportParam(url.searchParams.get('transport')),
        unixSocketDir: url.searchParams.get('unixSocketDir') ?? undefined,
      });
    } else {
      throw new PdbError(
        'Unsupported connection string scheme (use pdb:// or postgresql://)',
      );
    }
    try {
      await db.backend.queryResult('SELECT 1');
    } catch (err) {
      await db.close().catch(() => undefined);
      throw err instanceof PdbError ? err : new ConnectionError((err as Error).message);
    }
    return db;
  }

  private static initFields(db: ParticleDB, backend: Backend): void {
    db.backend = backend;
    db._vector = null;
    db._kv = null;
    db._rag = null;
    db._ai = null;
    db._ops = null;
  }

  /** The wire protocol this client uses: 'pdb' (native, default) or 'pg'. */
  get protocol(): 'pdb' | 'pg' {
    return this.backend.protocol;
  }

  /**
   * Execute a SQL query and return typed results.
   *
   * @param sql - The SQL statement (may contain $1, $2, ... placeholders).
   * @param params - Optional parameter values for the placeholders. On the
   *   native wire they are bound through a cached server-side prepared
   *   statement (MSG_PREPARE + MSG_EXECUTE).
   * @returns The query result with typed rows, row count (rows returned, or
   *   rows affected for DML) and column names.
   *
   * @example
   * ```ts
   * const { rows } = await db.query<{ count: number }>(
   *   'SELECT COUNT(*) as count FROM users'
   * );
   * ```
   */
  async query<T = Record<string, any>>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    return this.backend.queryResult<T>(sql, params);
  }

  /**
   * Execute a query and return only the first row, or null if empty.
   *
   * @param sql - The SQL statement.
   * @param params - Optional parameter values.
   * @returns The first row as a typed object, or null.
   */
  async queryOne<T = Record<string, any>>(sql: string, params?: any[]): Promise<T | null> {
    const result = await this.query<T>(sql, params);
    return result.rows.length > 0 ? result.rows[0] : null;
  }

  /**
   * Execute a SQL statement that modifies data.
   *
   * @param sql - The SQL statement (INSERT, UPDATE, DELETE, DDL).
   * @param params - Optional parameter values.
   * @returns The number of rows affected.
   */
  async execute(sql: string, params?: any[]): Promise<number> {
    return (await this.backend.queryResult(sql, params)).rowCount;
  }

  /**
   * Run one parameterized statement once per parameter set.
   *
   * On the native wire this is ONE round-trip: the statement is prepared
   * (cached) and the parameter sets travel as an Arrow IPC batch in a single
   * MSG_EXECUTE_BATCH frame (split only past the 16 MiB frame cap). Each set
   * autocommits on its own; if any set fails this rejects with
   * `BatchExecuteError` (its `rowCounts` shows which sets committed). On the
   * PG wire it runs one statement per set.
   *
   * @example
   * ```ts
   * await db.executeBatch('INSERT INTO t (id, name) VALUES ($1, $2)',
   *   [[1, 'a'], [2, 'b'], [3, null]]);
   * ```
   */
  async executeBatch(sql: string, rows: any[][], options?: ExecuteBatchOptions): Promise<BatchExecuteResult> {
    return this.backend.executeBatch(sql, rows, options);
  }

  /**
   * Iterate over a query's rows as an async iterable.
   *
   * The result is fetched in full and then yielded row by row; for
   * this method does not bound memory for very large results.
   */
  async *stream<T = Record<string, any>>(sql: string, params?: any[]): AsyncIterable<T> {
    const result = await this.backend.queryResult<T>(sql, params);
    for (const row of result.rows) {
      yield row;
    }
  }

  /**
   * Execute a function within a database transaction.
   *
   * The transaction is automatically committed if the function succeeds,
   * or rolled back if it throws an error. All statements run on one
   * connection.
   *
   * @example
   * ```ts
   * const result = await db.transaction(async (tx) => {
   *   await tx.execute('INSERT INTO accounts (id, balance) VALUES ($1, $2)', [1, 100]);
   *   await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [50, 2]);
   *   return 'done';
   * });
   * ```
   */
  async transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    return this.backend.transaction((tx) => fn(new Transaction(tx)));
  }

  /**
   * Run a statement and return positional rows with the server's column
   * descriptions (Arrow type and declared SQL type per column) -- the entry
   * point for driver layers such as ORM adapters. Native protocol only.
   */
  async queryArrays(sql: string, params?: any[], options?: DecodeOptions): Promise<RawQueryResult> {
    return this.native('queryArrays').queryArrays(sql, params, options);
  }

  /**
   * Check one connection out of the pool for exclusive use until its
   * `release()`, for callers that drive `BEGIN` / `COMMIT` themselves (ORM
   * transaction APIs). Application code should prefer {@link transaction}.
   * Native protocol only.
   */
  async reserve(): Promise<ReservedConnection> {
    return this.native('reserve').reserve();
  }

  private native(what: string): PdbWireClient {
    if (!(this.backend instanceof NativeBackend)) {
      throw new PdbError(`${what}() needs the native protocol; this client uses '${this.backend.protocol}'`);
    }
    return this.backend.client;
  }

  /** Access vector search operations. */
  get vector(): VectorOps {
    if (!this._vector) this._vector = new VectorOps(this.backend);
    return this._vector;
  }

  /** Access Redis-compatible key-value operations. */
  get kv(): KVOps {
    if (!this._kv) this._kv = new KVOps(this.backend);
    return this._kv;
  }

  /** Access RAG (Retrieval-Augmented Generation) query operations. */
  get rag(): RAGOps {
    if (!this._rag) this._rag = new RAGOps(this.backend);
    return this._rag;
  }

  /** Access AI function operations (embedding, generation, classification). */
  get ai(): AIOps {
    if (!this._ai) this._ai = new AIOps(this.backend);
    return this._ai;
  }

  /**
   * Access extended operations (time travel, materialized views,
   * changefeeds, geospatial, time-series, lakehouse).
   */
  get ops(): ExtendedOps {
    if (!this._ops) this._ops = new ExtendedOps(this.backend);
    return this._ops;
  }

  /**
   * Close the connection pool and release all resources.
   *
   * After calling close(), this client instance must not be used.
   */
  async close(): Promise<void> {
    await this.backend.close();
  }
}
