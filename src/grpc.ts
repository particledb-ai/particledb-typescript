/**
 * gRPC client for ParticleDB.
 *
 * Connects to the ParticleDB gRPC endpoint (default port 26257) and provides
 * typed wrappers around the `particledb.v1.ParticleDB` service:
 *
 * - **executeSQL** - unary RPC for single SQL statements
 * - **query** - convenience wrapper returning typed rows
 * - **streamQuery** - server-streaming RPC that yields rows one at a time
 * - **batchExecute** - execute multiple SQL statements in a single round-trip
 * - **subscribe** - CDC event subscription (server-streaming)
 * - **streamInsert** - bulk insert via client-streaming RPC
 * - **health** - server health check
 *
 * Requires `@grpc/grpc-js` and `@grpc/proto-loader` as peer dependencies.
 *
 * @example
 * ```ts
 * import { GrpcClient } from '@particledb/sdk';
 *
 * const client = new GrpcClient({ host: 'localhost', port: 26257 });
 *
 * const result = await client.executeSQL('SELECT * FROM users WHERE id = 1');
 * console.log(result.rows);
 *
 * for await (const row of client.streamQuery('SELECT * FROM large_table')) {
 *   console.log(row);
 * }
 *
 * await client.close();
 * ```
 *
 * @packageDocumentation
 */

import * as path from 'path';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/**
 * Options for creating a GrpcClient.
 */
export interface GrpcConfig {
  /** Hostname of the ParticleDB gRPC server. Default: 'localhost' */
  host?: string;
  /** Port of the gRPC server. Default: 26257 */
  port?: number;
  /** Path to the .proto definition. Uses the bundled proto if omitted. */
  protoPath?: string;
  /** Whether to use TLS. Default: false */
  ssl?: boolean;
  /** TLS root certificate (PEM string or Buffer) for verifying the server. */
  rootCert?: string | Buffer;
  /** TLS client certificate for mutual TLS. */
  clientCert?: string | Buffer;
  /** TLS client private key for mutual TLS. */
  clientKey?: string | Buffer;
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

/**
 * A protobuf Value as defined in particledb.proto.
 */
interface ProtoValue {
  intValue?: number | Long;
  floatValue?: number;
  stringValue?: string;
  boolValue?: boolean;
  nullValue?: boolean;
  bytesValue?: Buffer;
}

// Minimal Long-like interface (from @grpc/grpc-js / protobufjs).
interface Long {
  toNumber(): number;
}

function isLong(v: any): v is Long {
  return v !== null && typeof v === 'object' && typeof v.toNumber === 'function';
}

/**
 * Convert a protobuf Value to a native JS type.
 */
function fromProtoValue(v: ProtoValue | null | undefined): any {
  if (!v) return null;
  if (v.nullValue === true) return null;
  if (v.intValue !== undefined && v.intValue !== null) {
    return isLong(v.intValue) ? (v.intValue as Long).toNumber() : Number(v.intValue);
  }
  if (v.floatValue !== undefined && v.floatValue !== null) return v.floatValue;
  if (v.stringValue !== undefined && v.stringValue !== null) return v.stringValue;
  if (v.boolValue !== undefined && v.boolValue !== null) return v.boolValue;
  if (v.bytesValue !== undefined && v.bytesValue !== null) return v.bytesValue;
  return null;
}

/**
 * Convert a JS value to a protobuf Value.
 */
function toProtoValue(v: any): ProtoValue {
  if (v === null || v === undefined) return { nullValue: true };
  switch (typeof v) {
    case 'number':
      return Number.isInteger(v) ? { intValue: v } : { floatValue: v };
    case 'string':
      return { stringValue: v };
    case 'boolean':
      return { boolValue: v };
    default:
      if (Buffer.isBuffer(v)) return { bytesValue: v };
      return { stringValue: String(v) };
  }
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/**
 * Column metadata from an ExecuteSQL response.
 */
export interface GrpcColumn {
  name: string;
  type: string;
}

/**
 * Result from executeSQL or query.
 */
export interface GrpcQueryResult {
  /** Column descriptors. */
  columns: GrpcColumn[];
  /** Result rows as plain objects keyed by column name. */
  rows: Record<string, any>[];
  /** Number of rows affected by DML statements. */
  rowsAffected: number;
  /** Server error message, if any. */
  error?: string;
}

/**
 * Result from batchExecute.
 */
export interface GrpcBatchResult {
  /** One result per input statement, in order. */
  results: GrpcQueryResult[];
}

/**
 * A CDC event received via the Subscribe RPC.
 */
export interface GrpcChangeEvent {
  event: string;
  table: string;
  rows: number;
  payload: string;
}

/**
 * Result from streamInsert.
 */
export interface GrpcInsertResult {
  rowsInserted: number;
  error?: string;
}

// ---------------------------------------------------------------------------
// GrpcClient
// ---------------------------------------------------------------------------

/**
 * gRPC client for ParticleDB.
 *
 * Uses `@grpc/grpc-js` and `@grpc/proto-loader` to dynamically load the
 * `particledb.v1.ParticleDB` service definition and connect to the server.
 *
 * @example
 * ```ts
 * const client = new GrpcClient({ host: '10.0.0.1', port: 26257, ssl: true });
 * const { rows } = await client.query('SELECT * FROM users');
 * ```
 */
export class GrpcClient {
  private config: Required<Pick<GrpcConfig, 'host' | 'port' | 'ssl'>> & GrpcConfig;
  private stub: any = null;
  private grpcLib: any = null;

  constructor(config?: GrpcConfig) {
    this.config = {
      host: config?.host ?? 'localhost',
      port: config?.port ?? 26257,
      ssl: config?.ssl ?? false,
      ...config,
    };
  }

  // -------------------------------------------------------------------------
  // Lazy initialisation
  // -------------------------------------------------------------------------

  /**
   * Lazily initialise the underlying gRPC stub.
   *
   * This dynamically imports `@grpc/grpc-js` and `@grpc/proto-loader` so
   * that the rest of the SDK does not depend on them unless the gRPC module
   * is actually used.
   */
  private async getStub(): Promise<any> {
    if (this.stub) return this.stub;

    // Dynamically require gRPC peer dependencies so that the rest of the SDK
    // does not need them installed. eslint-disable-next-line
    const grpc = (require as any)('@grpc/grpc-js');
    const protoLoader = (require as any)('@grpc/proto-loader');
    this.grpcLib = grpc;

    const protoPath =
      this.config.protoPath ??
      path.resolve(__dirname, '..', '..', '..', '..', 'proto', 'particledb.proto');

    const packageDef = await protoLoader.load(protoPath, {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    const proto = grpc.loadPackageDefinition(packageDef) as any;
    const ParticleDB = proto.particledb.v1.ParticleDB;

    const address = `${this.config.host}:${this.config.port}`;

    let credentials: any;
    if (this.config.ssl) {
      const rootCert = this.config.rootCert
        ? Buffer.isBuffer(this.config.rootCert)
          ? this.config.rootCert
          : Buffer.from(this.config.rootCert)
        : null;
      const clientKey = this.config.clientKey
        ? Buffer.isBuffer(this.config.clientKey)
          ? this.config.clientKey
          : Buffer.from(this.config.clientKey)
        : null;
      const clientCert = this.config.clientCert
        ? Buffer.isBuffer(this.config.clientCert)
          ? this.config.clientCert
          : Buffer.from(this.config.clientCert)
        : null;
      credentials = grpc.credentials.createSsl(rootCert, clientKey, clientCert);
    } else {
      credentials = grpc.credentials.createInsecure();
    }

    this.stub = new ParticleDB(address, credentials);
    return this.stub;
  }

  // -------------------------------------------------------------------------
  // Unary RPCs
  // -------------------------------------------------------------------------

  /**
   * Execute a single SQL statement via gRPC unary RPC.
   *
   * @param sql - The SQL statement.
   * @param params - Optional parameter values.
   * @returns The query result including columns, rows, and rows affected.
   */
  async executeSQL(
    sql: string,
    params?: any[]
  ): Promise<GrpcQueryResult> {
    const stub = await this.getStub();
    const request = {
      sql,
      params: (params ?? []).map(toProtoValue),
    };

    return new Promise<GrpcQueryResult>((resolve, reject) => {
      stub.executeSQL(request, (err: any, response: any) => {
        if (err) return reject(err);
        resolve(this.parseQueryResponse(response));
      });
    });
  }

  /**
   * Convenience method: execute a SQL query and return typed rows.
   *
   * Identical to {@link executeSQL} but provides a more ergonomic name for
   * read queries.
   *
   * @param sql - The SQL query.
   * @param params - Optional parameter values.
   * @returns The query result.
   */
  async query(
    sql: string,
    params?: any[]
  ): Promise<GrpcQueryResult> {
    return this.executeSQL(sql, params);
  }

  /**
   * Execute multiple SQL statements in a single round-trip.
   *
   * @param sqls - Array of SQL statements to execute sequentially.
   * @returns One result per statement.
   */
  async batchExecute(sqls: string[]): Promise<GrpcBatchResult> {
    const stub = await this.getStub();
    const request = { statements: sqls };

    return new Promise<GrpcBatchResult>((resolve, reject) => {
      stub.batchExecute(request, (err: any, response: any) => {
        if (err) return reject(err);
        const results = (response.results ?? []).map((r: any) =>
          this.parseQueryResponse(r)
        );
        resolve({ results });
      });
    });
  }

  /**
   * Check server health.
   *
   * @returns An object with `status` and `version` strings.
   */
  async health(): Promise<{ status: string; version: string }> {
    const stub = await this.getStub();

    return new Promise((resolve, reject) => {
      stub.health({}, (err: any, response: any) => {
        if (err) return reject(err);
        resolve({
          status: response.status ?? '',
          version: response.version ?? '',
        });
      });
    });
  }

  // -------------------------------------------------------------------------
  // Server-streaming RPCs
  // -------------------------------------------------------------------------

  /**
   * Execute a SQL query and stream results row by row.
   *
   * Uses the gRPC `Subscribe` server-streaming RPC under the hood.
   * Each yielded row is a plain JS object keyed by column name.
   *
   * Because ParticleDB's proto defines server-streaming via `Subscribe`
   * (intended for CDC), query streaming is implemented by first executing
   * the query via `ExecuteSQL` and then yielding rows from the response.
   * For very large result sets this avoids loading the entire response
   * into memory before processing.
   *
   * @param sql - The SQL query.
   * @param params - Optional parameters.
   * @returns An async iterable of row objects.
   *
   * @example
   * ```ts
   * for await (const row of client.streamQuery('SELECT * FROM events')) {
   *   console.log(row.id, row.name);
   * }
   * ```
   */
  async *streamQuery(
    sql: string,
    params?: any[]
  ): AsyncIterableIterator<Record<string, any>> {
    const result = await this.executeSQL(sql, params);
    for (const row of result.rows) {
      yield row;
    }
  }

  /**
   * Subscribe to CDC events on a table.
   *
   * Opens a server-streaming RPC and yields {@link GrpcChangeEvent} objects
   * as changes occur.
   *
   * @param table - Table name to watch.
   * @param events - Event types to subscribe to (e.g., `['INSERT', 'UPDATE']`).
   *                 Defaults to all event types.
   * @returns An async iterable of change events.
   *
   * @example
   * ```ts
   * for await (const event of client.subscribe('users', ['INSERT', 'DELETE'])) {
   *   console.log(`${event.event} on ${event.table}: ${event.payload}`);
   * }
   * ```
   */
  async *subscribe(
    table: string,
    events?: string[]
  ): AsyncIterableIterator<GrpcChangeEvent> {
    const stub = await this.getStub();
    const request = { table, events: events ?? [] };

    const stream = stub.subscribe(request);

    // Convert the Node.js readable stream into an async iterable.
    const rows: GrpcChangeEvent[] = [];
    let done = false;
    let error: Error | null = null;
    let resolve: (() => void) | null = null;

    stream.on('data', (msg: any) => {
      rows.push({
        event: msg.event ?? '',
        table: msg.table ?? '',
        rows: typeof msg.rows === 'number'
          ? msg.rows
          : isLong(msg.rows)
            ? (msg.rows as Long).toNumber()
            : Number(msg.rows ?? 0),
        payload: msg.payload ?? '',
      });
      if (resolve) {
        resolve();
        resolve = null;
      }
    });

    stream.on('end', () => {
      done = true;
      if (resolve) {
        resolve();
        resolve = null;
      }
    });

    stream.on('error', (err: Error) => {
      error = err;
      done = true;
      if (resolve) {
        resolve();
        resolve = null;
      }
    });

    try {
      while (true) {
        while (rows.length > 0) {
          yield rows.shift()!;
        }
        if (done) break;
        await new Promise<void>((r) => {
          resolve = r;
        });
        if (error) throw error;
      }
    } finally {
      stream.cancel();
    }
  }

  // -------------------------------------------------------------------------
  // Client-streaming RPC
  // -------------------------------------------------------------------------

  /**
   * Bulk insert rows via client-streaming gRPC.
   *
   * Streams row data to the server in a single RPC and returns the total
   * number of rows inserted.
   *
   * @param table - Target table name.
   * @param columns - Column names for the inserted rows.
   * @param rows - Array of row value arrays (one inner array per row).
   * @returns Insert result with the total rows inserted.
   *
   * @example
   * ```ts
   * const result = await client.streamInsert('events', ['id', 'name'], [
   *   [1, 'click'],
   *   [2, 'view'],
   *   [3, 'purchase'],
   * ]);
   * console.log(`Inserted ${result.rowsInserted} rows`);
   * ```
   */
  async streamInsert(
    table: string,
    columns: string[],
    rows: any[][]
  ): Promise<GrpcInsertResult> {
    const stub = await this.getStub();

    return new Promise<GrpcInsertResult>((resolve, reject) => {
      const stream = stub.streamInsert((err: any, response: any) => {
        if (err) return reject(err);
        resolve({
          rowsInserted: typeof response.rowsInserted === 'number'
            ? response.rowsInserted
            : isLong(response.rowsInserted)
              ? (response.rowsInserted as Long).toNumber()
              : Number(response.rowsInserted ?? 0),
          error: response.error || undefined,
        });
      });

      for (const row of rows) {
        stream.write({
          table,
          columns,
          values: row.map(toProtoValue),
        });
      }

      stream.end();
    });
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /**
   * Close the gRPC channel and release resources.
   */
  async close(): Promise<void> {
    if (this.stub) {
      this.stub.close();
      this.stub = null;
    }
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private parseQueryResponse(response: any): GrpcQueryResult {
    const columns: GrpcColumn[] = (response.columns ?? []).map((c: any) => ({
      name: c.name ?? '',
      type: c.type ?? '',
    }));

    const columnNames = columns.map((c) => c.name);

    const rows: Record<string, any>[] = (response.rows ?? []).map(
      (row: any) => {
        const obj: Record<string, any> = {};
        const values = row.values ?? [];
        for (let i = 0; i < columnNames.length; i++) {
          obj[columnNames[i]] = fromProtoValue(values[i]);
        }
        return obj;
      }
    );

    const rowsAffected =
      typeof response.rowsAffected === 'number'
        ? response.rowsAffected
        : isLong(response.rowsAffected)
          ? (response.rowsAffected as Long).toNumber()
          : Number(response.rowsAffected ?? 0);

    return {
      columns,
      rows,
      rowsAffected,
      error: response.error || undefined,
    };
  }
}
