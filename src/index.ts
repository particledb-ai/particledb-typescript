/**
 * ParticleDB TypeScript/Node.js SDK
 *
 * Official client library for ParticleDB, providing typed SQL queries,
 * vector search, key-value operations, RAG pipelines, and AI functions.
 *
 * @example
 * ```ts
 * import { ParticleDB } from '@particledb/sdk';
 *
 * // PDB native wire (port 7173) by default; `protocol: 'pg'` opts into the
 * // PostgreSQL wire (needs the optional `pg` package).
 * const db = new ParticleDB({ host: 'localhost' });
 *
 * // SQL queries
 * const { rows } = await db.query('SELECT * FROM users WHERE id = $1', [1]);
 *
 * // Vector search
 * const results = await db.vector.search('docs', 'embedding', [0.1, 0.2, 0.3]);
 *
 * // Key-value
 * await db.kv.set('key', 'value');
 *
 * // RAG
 * const answer = await db.rag.query('What is ParticleDB?');
 *
 * // AI
 * const embedding = await db.ai.embed('Hello world');
 *
 * await db.close();
 *
 * // Streaming (WebSocket / SSE)
 * import { StreamingClient } from '@particledb/sdk';
 * const stream = new StreamingClient({ host: 'localhost', httpPort: 4000 });
 * await stream.connect();
 * stream.subscribe('cdc_users', (event) => console.log(event));
 *
 * // gRPC
 * import { GrpcClient } from '@particledb/sdk';
 * const grpc = new GrpcClient({ host: 'localhost', port: 26257 });
 * const result = await grpc.executeSQL('SELECT 1');
 * ```
 *
 * @packageDocumentation
 */

export { ParticleDB, Transaction } from './client';
export {
  PdbWireClient,
  TransactionContext,
  PdbError,
  ConnectionError,
  QueryError,
  TransactionRolledBackError,
  DiskFullError,
  BatchExecuteError,
  ReservedConnection,
  sqlstateForCode,
  sqlstateOf,
  DEFAULT_UNIX_SOCKET_DIR,
  unixSocketPath,
  isLocalHost,
  nativeEndpoints,
} from './pdb_wire';
export type { NativeEndpoint } from './pdb_wire';
export { ExtendedOps } from './ops';
export { VectorOps } from './vector';
export { KVOps } from './kv';
export { RAGOps } from './rag';
export { AIOps } from './ai';
export { StreamingClient } from './streaming';
export { GrpcClient } from './grpc';

export type {
  ParticleDBConfig,
  QueryResult,
  VectorSearchResult,
  VectorSearchOptions,
  RAGPipelineConfig,
  RAGQueryOptions,
  Row,
  ConnectionOptions,
  HealthResult,
  Queryable,
} from './types';

export type { ExecuteBatchOptions } from './client';
export type { BatchExecuteResult, BatchRowError, RawQueryResult } from './pdb_wire';
export type { ParamType, ResultField, DecodeOptions } from './arrow_ipc';

export type {
  StreamingConfig,
  ChangeEvent,
  StreamRow,
  SubscriptionCallback,
  RowCallback,
  EventHandler,
} from './streaming';

export type {
  GrpcConfig,
  GrpcColumn,
  GrpcQueryResult,
  GrpcBatchResult,
  GrpcChangeEvent,
  GrpcInsertResult,
} from './grpc';
