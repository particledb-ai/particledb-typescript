import type { ConnectionOptions as TLSConnectionOptions } from 'node:tls';

/**
 * Configuration options for connecting to a ParticleDB instance.
 */
export interface ParticleDBConfig {
  /**
   * Wire protocol. Default: 'pdb' -- the PDB native wire (binary frames,
   * Arrow IPC results, native prepared statements and EXECUTE_BATCH).
   * 'pg' selects the PostgreSQL wire for legacy setups; it needs the optional
   * `pg` package installed (`npm install pg`), which is loaded only then.
   */
  protocol?: 'pdb' | 'pg';
  /** Hostname of the ParticleDB server. Default: 'localhost' */
  host?: string;
  /** Port of the ParticleDB server. Default: 7173 (native), 5432 when protocol is 'pg'. */
  port?: number;
  /** Username for authentication. Default: 'particledb' */
  user?: string;
  /** Password for authentication. */
  password?: string;
  /**
   * Database name. Used by the 'pg' protocol only (default 'particledb'); the
   * native wire has no database selection and ignores it.
   */
  database?: string;
  /** Maximum number of connections in the pool. Default: 10 */
  poolSize?: number;
  /** TLS with certificate and hostname verification. Pass Node TLS options for a private CA or client certificate. */
  ssl?: boolean | TLSConnectionOptions;
  /** Native protocol: per-request timeout in milliseconds. Default: 30000. */
  timeoutMs?: number;
  /** Native protocol: 'auto' (Unix socket on localhost, else TCP) | 'unix' | 'tcp'. Default: 'auto'. */
  transport?: 'auto' | 'unix' | 'tcp';
  /** Native protocol: the server's `--unix-socket-dir`. Default: '/tmp'. */
  unixSocketDir?: string;
}

/**
 * Result of a SQL query execution.
 */
export interface QueryResult<T = Record<string, any>> {
  /** Array of result rows. */
  rows: T[];
  /** Number of rows returned or affected. */
  rowCount: number;
  /** Column names in the result set. */
  columns: string[];
}

/**
 * A single vector search result with its distance from the query vector.
 */
export interface VectorSearchResult {
  /** The matched row data (all non-vector columns). */
  row: Record<string, any>;
  /** Distance from the query vector (lower = more similar). */
  distance: number;
}

/**
 * Options for vector similarity search.
 */
export interface VectorSearchOptions {
  /** SQL WHERE clause to filter candidates before vector search. */
  filter?: string;
  /** Number of nearest neighbors to return. Default: 10 */
  k?: number;
}

/**
 * Configuration for a RAG (Retrieval-Augmented Generation) pipeline.
 */
export interface RAGPipelineConfig {
  /** Table containing the document vectors and text. */
  table: string;
  /** Column containing the vector embeddings. */
  vectorCol: string;
  /** Column containing the raw text. */
  textCol: string;
  /** Number of nearest neighbors to retrieve. Default: 5 */
  k?: number;
  /** RAG mode (e.g., 'simple', 'rerank'). Default: 'simple' */
  mode?: string;
}

/**
 * Options for a RAG query.
 */
export interface RAGQueryOptions {
  /** Named pipeline to use. */
  pipeline?: string;
  /** Table to query (if not using a named pipeline). */
  table?: string;
  /** Vector column name. */
  vectorCol?: string;
  /** Text column name. */
  textCol?: string;
  /** Number of nearest neighbors. */
  k?: number;
}

/**
 * A single result row, keyed by column name. Values are decoded from the
 * server's Arrow IPC response (numbers, strings, booleans, nested objects…).
 */
export type Row = Record<string, any>;

/**
 * Connection options for the PDB native wire client ({@link PdbWireClient}).
 *
 * The native wire listens on its own port (default 7173), independent of the
 * PostgreSQL wire (5432) and the HTTP API. For a local host (`localhost`,
 * `127.x`, `::1`) the client connects over the server's Unix domain socket
 * `<unixSocketDir>/.s.PDB.<port>` when that file exists, else TCP.
 */
export interface ConnectionOptions {
  /** Server hostname or IP. Default: 'localhost'. */
  host?: string;
  /** PDB native wire port. Default: 7173. */
  port?: number;
  /** Maximum number of pooled connections. Default: min(cpuCount, 16). */
  poolSize?: number;
  /** Per-request timeout in milliseconds. Default: 30000. */
  timeoutMs?: number;
  /**
   * Transport selection. 'auto' (default): the Unix socket for a local host
   * when it exists, else TCP; a socket that exists but does not answer falls
   * back to TCP. 'unix': the socket only. 'tcp': TCP only (the opt-out).
   */
  transport?: 'auto' | 'unix' | 'tcp';
  /** Directory of the server's sockets (its `--unix-socket-dir`). Default: '/tmp'. */
  unixSocketDir?: string;
  /** Require TLS before sending credentials or requests. true verifies the server using system roots. */
  ssl?: boolean | TLSConnectionOptions;
  /** Collect client-side metrics. Default: true. */
  metricsEnabled?: boolean;
  /**
   * Retries of a failure to OPEN a connection (connect, HELLO handshake) --
   * the only failures that happen before any byte of a request is written.
   * A request already being sent is never retried: a transport error or
   * timeout after that rejects with `ConnectionError` (the server may have
   * executed it) and the connection is closed. Default: 3.
   */
  maxRetries?: number;
  /** Base backoff between retries in milliseconds. Default: 100. */
  retryDelayMs?: number;
  /**
   * User for MSG_AUTH. Sent only together with `password`. Default:
   * 'particledb'.
   */
  user?: string;
  /**
   * Password for MSG_AUTH. When unset no AUTH frame is sent (a server started
   * with `--no-auth`).
   */
  password?: string;
  /**
   * Server-side prepared statements kept per connection (keyed by SQL text,
   * least recently used is closed with MSG_CLOSE_STMT). Default: 256.
   */
  statementCacheSize?: number;
}

/**
 * Anything with a pg-style `query(sql, params)` returning `{ rows, rowCount }`.
 * The helper modules (vector, kv, rag, ai, ops) run on this; `ParticleDB`
 * passes them its native-wire executor by default, or a `pg` Pool when the
 * PG protocol was selected explicitly.
 */
export interface Queryable {
  query(sql: string, params?: any[]): Promise<{ rows: any[]; rowCount: number | null }>;
}

/**
 * Health summary returned by {@link PdbWireClient.health}.
 */
export interface HealthResult {
  /** Whether the last probe reached the server. */
  connected: boolean;
  /** Round-trip latency of the probe, in milliseconds. */
  latencyMs: number;
  /** Active transport/protocol, e.g. 'pdb_wire'. */
  protocol: string;
  /** Native wire: the socket kind the probing connection rode, when connected. */
  transport?: 'unix' | 'tcp';
  /** Server protocol version reported at HELLO, when connected. */
  serverVersion?: number;
  /** Connections currently checked out of the pool. */
  poolActive: number;
  /** Idle connections available in the pool. */
  poolIdle: number;
}
