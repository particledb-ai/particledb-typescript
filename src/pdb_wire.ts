/**
 * PDB Native Wire Protocol client for TypeScript/Node.js.
 *
 * Uses PDB's binary protocol for maximum performance.
 * Auto-detects Unix sockets on localhost, falls back to TCP.
 * Supports connection pooling, retry with exponential backoff (of
 * connection establishment only -- a request that may have reached the
 * server is never resent), timeouts, and optional Prometheus metrics.
 *
 * @example
 * ```ts
 * import { PdbWireClient } from '@particledb/sdk';
 *
 * const db = new PdbWireClient({ host: 'localhost', port: 7173 });
 *
 * // SELECT - returns typed rows
 * const rows = await db.query('SELECT * FROM users WHERE id = $1', [42]);
 *
 * // DML - returns affected row count
 * const n = await db.execute("INSERT INTO users VALUES (1, 'alice')");
 *
 * // Multiple statements in one round-trip
 * await db.batch(['BEGIN', "INSERT INTO users VALUES (2, 'bob')", 'COMMIT']);
 *
 * // Transaction with auto-commit/rollback
 * await db.transaction(async (tx) => {
 *   await tx.execute("INSERT INTO orders VALUES (1, 100)");
 *   await tx.execute("UPDATE balances SET amount = amount - 100 WHERE id = 1");
 * });
 *
 * await db.close();
 * ```
 */

import * as fs from 'fs';
import * as net from 'net';
import * as tls from 'node:tls';
import {inspect} from 'node:util';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';
import { ConnectionOptions, Row, HealthResult, QueryResult } from './types';
import {
  ArrowCodecError,
  ParamType,
  ResultField,
  DecodeOptions,
  decodeArrowStream,
  encodeArrowStream,
  inferParamType,
  normalizeParam,
} from './arrow_ipc';

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

const HEADER_SIZE = 8; // [u32 length LE][u16 stream_id LE][u8 msg_type][u8 flags]

// Client → Server message types
const MSG_HELLO = 0x00;
const MSG_QUERY = 0x01;
const MSG_BATCH = 0x02;
const MSG_PING = 0x03;
const MSG_CLOSE = 0x04;
const MSG_PREPARE = 0x05;
const MSG_EXECUTE = 0x06;
const MSG_EXECUTE_BATCH = 0x07;
const MSG_CLOSE_STMT = 0x08;
const MSG_AUTH = 0x09;

// Server → Client message types
const MSG_HELLO_OK = 0x80;
const MSG_ARROW_RESULT = 0x81;
const MSG_OK = 0x82;
const MSG_ERROR = 0x83;
const MSG_PONG = 0x84;
const MSG_BATCH_RESULT = 0x85;
const MSG_ARROW_STREAM = 0x86;
const MSG_PREPARE_OK = 0x89;

// Flags
const FLAG_MORE_CHUNKS = 0x02;
/** On the OK / BATCH_RESULT answering a COMMIT that rolled back a FAILED block. */
const FLAG_TXN_ROLLED_BACK = 0x40;

// HELLO fields
const HELLO_VERSION = 0x01;
const FMT_ARROW_IPC = 0x01;

// Defaults
const DEFAULT_PORT = 7173;
const DEFAULT_POOL_SIZE = Math.min(os.cpus().length, 16);
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_DELAY_MS = 100;
const DEFAULT_STATEMENT_CACHE_SIZE = 256;
/** Server cap on one frame's payload (`MAX_PAYLOAD_SIZE` in the server). */
const MAX_PAYLOAD_SIZE = 16 * 1024 * 1024;
/** Rows per EXECUTE_BATCH frame before byte-size splitting kicks in. */
const MAX_BATCH_ROWS_PER_FRAME = 65_536;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/**
 * Base class for all PDB client errors. Always has a numeric `code`.
 */
export class PdbError extends Error {
  readonly code: number;
  constructor(message: string, code: number = 0) {
    super(message);
    this.code = code;
    this.name = 'PdbError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown when a network connection cannot be established or drops unexpectedly.
 */
export class ConnectionError extends PdbError {
  constructor(message: string, code: number = 0) {
    super(message, code);
    this.name = 'ConnectionError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * A NEW connection failed to connect or to complete the HELLO handshake. No
 * byte of any request was written on it, so the request that needed it may
 * be retried. Every other transport failure is ambiguous and is not retried.
 * @internal
 */
class ConnectFailedError extends ConnectionError {}

/**
 * Thrown when the server returns an error response to a query.
 */
export class QueryError extends PdbError {
  /**
   * The five-character SQLSTATE of a server error (`23505`, `40001`,
   * `42P01`, ...), decoded from the frame's error code and the engine's
   * `[XXXXX]` marker; undefined for errors raised on the client.
   */
  readonly sqlstate?: string;
  constructor(message: string, code: number = 0, sqlstate?: string) {
    super(message, code);
    this.name = 'QueryError';
    if (sqlstate !== undefined) this.sqlstate = sqlstate;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ---------------------------------------------------------------------------
// Wire error code -> SQLSTATE (the inverse of the server's
// sqlstate_to_wire_code; the same table as the Python SDK's
// `_protocol.sqlstate_for_code` and the JDBC driver's `PdbSqlState.BAND`).
// ---------------------------------------------------------------------------

/** Letter-bearing SQLSTATEs travel in the 6xxxx band. */
const SQLSTATE_BAND: Record<number, string> = {
  60000: 'XX000', 60001: '42P01', 60002: '42P02', 60003: '25P02', 60004: '25P01',
  60005: '40P01', 60006: '55P03', 60007: '0A000', 60008: '22P02', 60009: '3B001',
  60010: '28P01', 60011: '22P04', 60012: '2201B', 60013: '2201E', 60014: '2201F',
  60015: '2201G', 60016: '2202E', 60017: '2200H', 60018: '22P05', 60019: '2BP01',
  60020: '3D000', 60021: '3F000', 60022: '42P05', 60023: '42P06', 60024: '42P07',
  60025: '42P10', 60026: '42P16', 60027: '42P17', 60028: '42P18', 60029: '42P19',
  60030: '42P20', 60031: '42P21', 60032: '428C9', 60033: '55P02', 60034: '57P01',
  60035: '57P03', 60036: '08P01', 60037: 'P0001',
};

/** Protocol-level codes that are not numeric SQLSTATEs. */
const SQLSTATE_PROTOCOL: Record<number, string> = {
  0x00: '08006', 0x01: '08P01', 0x02: '08P01', 0x03: '08P01', 0x04: '08P01',
  0x05: '08P01', 0x06: '08P01', 0x07: '57014', 0x08: '08006', 0x10: '08004',
  0x11: '53300', 0x12: '08004', 0x13: '28000', 0x14: '28P01', 0x15: '22023',
  26: '26000', 1064: 'HY000',
};

/** The SQLSTATE a wire error code stands for; never undefined. */
export function sqlstateForCode(code: number): string {
  code &= 0xffff;
  if (code >= 60000) return SQLSTATE_BAND[code] ?? 'HY000';
  const p = SQLSTATE_PROTOCOL[code];
  if (p !== undefined) return p;
  if (code >= 10000) return String(code);
  if (code >= 1000) return '0' + String(code); // a leading-zero SQLSTATE (02000, 08006, ...)
  return 'HY000';
}

const isSqlstate = (s: string): boolean => /^[0-9A-Z]{5}$/.test(s);

/**
 * The SQLSTATE of a server ERROR frame: an exact class the engine wrote as a
 * `[XXXXX]` marker (leading, or trailing) wins over the frame code, which
 * cannot carry letter-bearing classes outside the band; then the code; then a
 * leading `XXXXX: ` prefix when the code is generic.
 */
export function sqlstateOf(code: number, message: string): string {
  const text = (message ?? '').trim();
  const head = text.indexOf('[');
  if (head >= 0 && head <= 16 && text.slice(head + 6, head + 8) === '] ' && isSqlstate(text.slice(head + 1, head + 6))) {
    return text.slice(head + 1, head + 6);
  }
  if (text.endsWith(']')) {
    const i = text.lastIndexOf('[');
    if (i >= 0 && isSqlstate(text.slice(i + 1, -1))) return text.slice(i + 1, -1);
  }
  const state = sqlstateForCode(code);
  if (state === 'HY000' && text.length > 7 && text.slice(5, 7) === ': ' && isSqlstate(text.slice(0, 5))) {
    return text.slice(0, 5);
  }
  return state;
}

/**
 * Thrown by `transaction()` when the server answered COMMIT by rolling the
 * transaction back because a statement inside it had failed (an error the
 * callback caught and did not rethrow). Nothing was committed.
 */
export class TransactionRolledBackError extends QueryError {
  constructor() {
    super('COMMIT did not commit: the server rolled back the transaction because a statement in it had failed');
    this.name = 'TransactionRolledBackError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Thrown when the server signals a disk-full condition (error code 28).
 * Error code 28 is ENOSPC on POSIX systems.
 */
export class DiskFullError extends PdbError {
  constructor(message: string) {
    super(message, 28);
    this.name = 'DiskFullError';
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// ---------------------------------------------------------------------------
// Metrics (no-op if prom-client is unavailable)
// ---------------------------------------------------------------------------

/** All 12 client-side metrics tracked by PdbWireClient. */
interface Metrics {
  connectionsCreated: number;
  connectionsClosed: number;
  connectionsErrored: number;
  queriesTotal: number;
  queriesErrored: number;
  queriesRetried: number;
  batchesTotal: number;
  bytesRead: number;
  bytesWritten: number;
  rowsReceived: number;
  latencySumMs: number;
  poolWaits: number;
}

function createMetrics(): Metrics {
  return {
    connectionsCreated: 0,
    connectionsClosed: 0,
    connectionsErrored: 0,
    queriesTotal: 0,
    queriesErrored: 0,
    queriesRetried: 0,
    batchesTotal: 0,
    bytesRead: 0,
    bytesWritten: 0,
    rowsReceived: 0,
    latencySumMs: 0,
    poolWaits: 0,
  };
}

/** Format metrics as Prometheus text exposition format. */
function formatPrometheus(m: Metrics): string {
  const lines: string[] = [
    '# HELP pdb_connections_created_total Total TCP/Unix connections opened',
    '# TYPE pdb_connections_created_total counter',
    `pdb_connections_created_total ${m.connectionsCreated}`,
    '# HELP pdb_connections_closed_total Total connections closed',
    '# TYPE pdb_connections_closed_total counter',
    `pdb_connections_closed_total ${m.connectionsClosed}`,
    '# HELP pdb_connections_errored_total Total connections lost due to error',
    '# TYPE pdb_connections_errored_total counter',
    `pdb_connections_errored_total ${m.connectionsErrored}`,
    '# HELP pdb_queries_total Total queries sent',
    '# TYPE pdb_queries_total counter',
    `pdb_queries_total ${m.queriesTotal}`,
    '# HELP pdb_queries_errored_total Total queries that returned an error',
    '# TYPE pdb_queries_errored_total counter',
    `pdb_queries_errored_total ${m.queriesErrored}`,
    '# HELP pdb_queries_retried_total Total query retry attempts',
    '# TYPE pdb_queries_retried_total counter',
    `pdb_queries_retried_total ${m.queriesRetried}`,
    '# HELP pdb_batches_total Total batch requests sent',
    '# TYPE pdb_batches_total counter',
    `pdb_batches_total ${m.batchesTotal}`,
    '# HELP pdb_bytes_read_total Total bytes received from server',
    '# TYPE pdb_bytes_read_total counter',
    `pdb_bytes_read_total ${m.bytesRead}`,
    '# HELP pdb_bytes_written_total Total bytes sent to server',
    '# TYPE pdb_bytes_written_total counter',
    `pdb_bytes_written_total ${m.bytesWritten}`,
    '# HELP pdb_rows_received_total Total rows decoded from Arrow IPC',
    '# TYPE pdb_rows_received_total counter',
    `pdb_rows_received_total ${m.rowsReceived}`,
    '# HELP pdb_query_latency_ms_total Sum of query latencies in milliseconds',
    '# TYPE pdb_query_latency_ms_total counter',
    `pdb_query_latency_ms_total ${m.latencySumMs}`,
    '# HELP pdb_pool_waits_total Times a caller waited for a free connection',
    '# TYPE pdb_pool_waits_total counter',
    `pdb_pool_waits_total ${m.poolWaits}`,
  ];
  return lines.join('\n') + '\n';
}

// ---------------------------------------------------------------------------
// Arrow IPC decoding
// ---------------------------------------------------------------------------

/**
 * Decode an Arrow IPC result stream into column names and rows with the
 * built-in decoder (`./arrow_ipc`). Only when the stream uses an Arrow type
 * that decoder does not handle, and the optional `apache-arrow` package is
 * installed, is the stream handed to `apache-arrow` instead; otherwise the
 * unsupported type is an error (never a silently raw or empty result).
 */
function decodeArrowResult(buf: Buffer): { columns: string[]; rows: Row[] } {
  try {
    return decodeArrowStream(buf);
  } catch (err) {
    if (!(err instanceof ArrowCodecError)) throw err;
    if (!err.unsupported) throw new PdbError(`protocol error: ${err.message}`);
    let arrow: any;
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      arrow = require('apache-arrow');
    } catch {
      throw new PdbError(`${err.message} (install the optional 'apache-arrow' package to decode it)`);
    }
    const table = arrow.tableFromIPC(buf);
    const columns: string[] = table.schema.fields.map((f: { name: string }) => f.name);
    const rows: Row[] = table.toArray().map((r: Record<string, any>) => ({ ...r }));
    return { columns, rows };
  }
}

/** Decode an Arrow IPC result stream into rows. */
function parseArrowIpc(buf: Buffer): Row[] {
  return decodeArrowResult(buf).rows;
}

/**
 * A statement's result with positional rows and the server's column
 * descriptions -- what a driver layer (an ORM adapter) needs: two columns
 * with the same label stay apart, and each column carries its Arrow type and
 * declared SQL type (`fields[i].decl`).
 */
export interface RawQueryResult {
  /** One entry per result column, in order (empty for a statement without rows). */
  fields: ResultField[];
  /** Column labels, in order. */
  columns: string[];
  /** Rows as arrays, `rows[r][c]` for `fields[c]`. */
  rows: any[][];
  /** Rows returned, or rows affected by a statement without a result set. */
  rowCount: number;
  /** True when the reply carried a result set (even an empty one). */
  hasResultSet: boolean;
  /**
   * True when this statement was a COMMIT that the server answered by
   * ROLLING BACK a transaction block in which a statement had already failed
   * (PostgreSQL's `ROLLBACK` command tag for such a COMMIT).
   */
  rolledBack: boolean;
}

function frameToRaw(frame: PdbFrame, opts?: DecodeOptions): RawQueryResult {
  const rolledBack = (frame.flags & FLAG_TXN_ROLLED_BACK) !== 0;
  if (frame.msgType === MSG_ARROW_RESULT) {
    let d;
    try {
      d = decodeArrowStream(frame.data, 'array', opts);
    } catch (err) {
      if (err instanceof ArrowCodecError) throw new PdbError(`protocol error: ${err.message}`);
      throw err;
    }
    return { fields: d.fields, columns: d.columns, rows: d.rows, rowCount: d.rows.length, hasResultSet: true, rolledBack };
  }
  if (frame.msgType === MSG_OK) {
    const affected = frame.data.length >= 8 ? Number(frame.data.readBigUInt64LE(0)) : 0;
    return { fields: [], columns: [], rows: [], rowCount: affected, hasResultSet: false, rolledBack };
  }
  throw new PdbError(`protocol error: unexpected reply 0x${frame.msgType.toString(16)}`);
}

/** Shape one response frame as a {@link QueryResult}. */
function frameToResult<T>(frame: PdbFrame): QueryResult<T> {
  if (frame.msgType === MSG_ARROW_RESULT) {
    const { columns, rows } = decodeArrowResult(frame.data);
    return { rows: rows as T[], rowCount: rows.length, columns };
  }
  if (frame.msgType === MSG_OK) {
    const affected = frame.data.length >= 8 ? Number(frame.data.readBigUInt64LE(0)) : 0;
    return { rows: [], rowCount: affected, columns: [] };
  }
  throw new PdbError(`protocol error: unexpected reply 0x${frame.msgType.toString(16)}`);
}

// ---------------------------------------------------------------------------
// Prepared-statement parameter encoding
// ---------------------------------------------------------------------------

/** Encode one parameter set as a 1-row Arrow IPC stream (MSG_EXECUTE). */
function encodeParamRow(params: any[]): Buffer {
  const columns = params.map((raw, i) => {
    const v = normalizeParam(raw);
    return { name: `p${i + 1}`, type: inferParamType([v], `$${i + 1}`), values: [v] };
  });
  return encodeArrowStream(columns);
}

/** Column types for an EXECUTE_BATCH: explicit, or inferred over every row. */
function batchParamTypes(rows: any[][], width: number, types?: ParamType[]): ParamType[] {
  if (types) {
    if (types.length !== width) {
      throw new PdbError(`executeBatch: ${types.length} types given for ${width} parameters`);
    }
    return types;
  }
  const out: ParamType[] = [];
  for (let c = 0; c < width; c++) {
    out.push(inferParamType(rows.map((r) => r[c]), `$${c + 1}`));
  }
  return out;
}

/** `[u32 LE name_len][name]` prefix of PREPARE / EXECUTE / CLOSE_STMT payloads. */
function nameHeader(name: string): Buffer {
  const nb = Buffer.from(name, 'utf-8');
  const hdr = Buffer.alloc(4);
  hdr.writeUInt32LE(nb.length, 0);
  return Buffer.concat([hdr, nb]);
}

// ---------------------------------------------------------------------------
// EXECUTE_BATCH results
// ---------------------------------------------------------------------------

/** Successful outcome of {@link PdbWireClient.executeBatch}. */
export interface BatchExecuteResult {
  /** Rows affected (DML) or returned (query) by each parameter set, in order. */
  rowCounts: number[];
  /** Sum of `rowCounts`. */
  totalRowCount: number;
}

/** One failed parameter set of an EXECUTE_BATCH. */
export interface BatchRowError {
  /** Index of the parameter set in the caller's `rows`. */
  row: number;
  /** Server error message. */
  message: string;
}

/**
 * Thrown by `executeBatch` when at least one parameter set failed.
 *
 * Outside a transaction every parameter set autocommits on its own, so the
 * sets whose `rowCounts` entry is a number DID commit. Inside a transaction
 * the server stops at the first failure and the transaction is aborted;
 * `rowCounts` is `null` for every set that failed or was not attempted.
 */
export class BatchExecuteError extends QueryError {
  readonly rowCounts: Array<number | null>;
  readonly errors: BatchRowError[];
  constructor(rowCounts: Array<number | null>, errors: BatchRowError[]) {
    const first = errors[0];
    super(
      `executeBatch: ${errors.length} of ${rowCounts.length} parameter set(s) failed; ` +
        `first at row ${first.row}: ${first.message}`,
    );
    this.name = 'BatchExecuteError';
    this.rowCounts = rowCounts;
    this.errors = errors;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

type BatchEntry = { ok: true; count: number } | { ok: false; message: string };

/**
 * Decode an EXECUTE_BATCH reply (`BATCH_RESULT`) for `sent` parameter sets.
 * One entry per set attempted: an autocommit batch attempts every set; inside
 * a transaction the server stops after the first failure, which must then be
 * the last entry. Any other shape is a protocol error.
 */
function decodeExecuteBatchResult(frame: PdbFrame, sent: number): BatchEntry[] {
  if (frame.msgType !== MSG_BATCH_RESULT) {
    throw new PdbError(
      `protocol error: expected BATCH_RESULT (0x85) for EXECUTE_BATCH, got 0x${frame.msgType.toString(16)}`,
    );
  }
  const buf = frame.data;
  if (buf.length < 4) throw new PdbError('protocol error: BATCH_RESULT payload too short');
  const count = buf.readUInt32LE(0);
  if (count > sent) throw new PdbError(`protocol error: BATCH_RESULT has ${count} entries for ${sent} parameter sets`);
  const out: BatchEntry[] = [];
  let off = 4;
  for (let i = 0; i < count; i++) {
    if (off + 1 > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
    const kind = buf[off++];
    if (kind === ENTRY_OK) {
      if (off + 8 > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
      out.push({ ok: true, count: Number(buf.readBigUInt64LE(off)) });
      off += 8;
    } else if (kind === ENTRY_ARROW || kind === ENTRY_ERROR) {
      if (off + 4 > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
      const len = buf.readUInt32LE(off);
      off += 4;
      if (off + len > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
      const body = buf.subarray(off, off + len);
      off += len;
      if (kind === ENTRY_ERROR) out.push({ ok: false, message: body.toString('utf-8') });
      else out.push({ ok: true, count: decodeArrowResult(Buffer.from(body)).rows.length });
    } else {
      throw new PdbError(`protocol error: unknown BATCH_RESULT entry type 0x${kind.toString(16)}`);
    }
  }
  if (count < sent && (count === 0 || out[count - 1].ok)) {
    throw new PdbError(
      `protocol error: BATCH_RESULT has ${count} entries for ${sent} parameter sets and does not end in an error`,
    );
  }
  return out;
}

// ---------------------------------------------------------------------------
// BATCH_RESULT decoding
// ---------------------------------------------------------------------------

const ENTRY_OK = 0x82;
const ENTRY_ARROW = 0x81;
const ENTRY_ERROR = 0x83;

/**
 * Validate a BATCH_RESULT reply to a batch of `sent` statements.
 *
 * Payload: [u32 LE count] then `count` entries of [u8 type][body]:
 * OK = u64 LE affected rows, ARROW = u32 LE len + Arrow IPC, ERROR = u32 LE
 * len + UTF-8 message. The server stops at the first failing statement, so
 * the failure is the LAST entry and `count` is below `sent`.
 *
 * Throws `QueryError` (server message) for an ERROR entry, and `PdbError`
 * for any reply that does not account for every statement sent.
 */
function decodeBatchResult(frame: PdbFrame, sent: number): void {
  if (frame.msgType !== MSG_BATCH_RESULT) {
    throw new PdbError(
      `protocol error: expected BATCH_RESULT (0x85) for a batch, got 0x${frame.msgType.toString(16)}`,
    );
  }
  const buf = frame.data;
  if (buf.length < 4) {
    throw new PdbError(`protocol error: BATCH_RESULT payload too short (${buf.length} bytes)`);
  }
  const count = buf.readUInt32LE(0);
  let off = 4;
  for (let i = 0; i < count; i++) {
    if (off + 1 > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
    const kind = buf[off++];
    if (kind === ENTRY_OK) {
      if (off + 8 > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
      off += 8;
    } else if (kind === ENTRY_ARROW || kind === ENTRY_ERROR) {
      if (off + 4 > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
      const len = buf.readUInt32LE(off);
      off += 4;
      if (off + len > buf.length) throw new PdbError(`protocol error: BATCH_RESULT truncated at entry ${i}`);
      if (kind === ENTRY_ERROR) {
        throw new QueryError(buf.subarray(off, off + len).toString('utf-8'));
      }
      off += len;
    } else {
      throw new PdbError(`protocol error: unknown BATCH_RESULT entry type 0x${kind.toString(16)}`);
    }
  }
  if (count !== sent) {
    throw new PdbError(
      `protocol error: BATCH_RESULT has ${count} entries for ${sent} statements and no error entry`,
    );
  }
}

// ---------------------------------------------------------------------------
// Wire frame type
// ---------------------------------------------------------------------------

interface PdbFrame {
  streamId: number;
  msgType: number;
  flags: number;
  data: Buffer;
}

// ---------------------------------------------------------------------------
// Low-level Connection
// ---------------------------------------------------------------------------

/**
 * A single physical connection to a PDB server.
 * Handles framing, HELLO handshake, and streaming chunked Arrow responses.
 * @internal
 */
/** The server's default `--unix-socket-dir`. */
export const DEFAULT_UNIX_SOCKET_DIR = '/tmp';

/** The server's native-wire socket for `port`: `<dir>/.s.PDB.<port>`. */
export function unixSocketPath(port: number, dir: string = DEFAULT_UNIX_SOCKET_DIR): string {
  return path.join(dir, `.s.PDB.${port}`);
}

/**
 * `localhost` or a loopback IP literal (127.0.0.0/8, `::1`). No DNS lookup:
 * only these literals get the Unix socket first.
 */
export function isLocalHost(host: string): boolean {
  if (host.toLowerCase() === 'localhost') return true;
  const h = host.replace(/^\[|\]$/g, '');
  if (net.isIPv4(h)) return h.split('.')[0] === '127';
  if (net.isIPv6(h)) return h === '::1' || /^(0{0,4}:){7}0{0,3}1$/.test(h) || h === '0:0:0:0:0:0:0:1';
  return false;
}

/** One endpoint to try. `fallback` = a failure moves on to the next one. */
export type NativeEndpoint =
  | { kind: 'unix'; path: string; fallback: boolean }
  | { kind: 'tcp'; host: string; port: number; fallback: false };

/**
 * The endpoints to try, in order. `'auto'`: a local host tries the server's
 * socket first when the file exists, then TCP; anything else is TCP only.
 * `'unix'`: the socket only (any host). `'tcp'`: TCP only.
 */
export function nativeEndpoints(
  host: string,
  port: number,
  transport: 'auto' | 'unix' | 'tcp' = 'auto',
  unixSocketDir: string = DEFAULT_UNIX_SOCKET_DIR,
  exists: (p: string) => boolean = fs.existsSync,
): NativeEndpoint[] {
  const sock = unixSocketPath(port, unixSocketDir);
  const tcp: NativeEndpoint = { kind: 'tcp', host: host.replace(/^\[|\]$/g, ''), port, fallback: false };
  if (transport === 'unix') return [{ kind: 'unix', path: sock, fallback: false }];
  if (transport === 'auto' && isLocalHost(host) && exists(sock)) {
    return [{ kind: 'unix', path: sock, fallback: true }, tcp];
  }
  return [tcp];
}

class Connection {
  private socket: net.Socket;
  /** Which transport this connection rides. */
  readonly via: 'unix' | 'tcp';
  private readBuf: Buffer = Buffer.alloc(0);
  private metrics: Metrics;

  // One pending request at a time (pipelined requests not supported by protocol).
  private pendingResolve?: (frame: PdbFrame) => void;
  private pendingReject?: (err: Error) => void;
  private pendingStreamChunks?: Buffer[];

  /** Whether the connection is healthy (not destroyed). */
  isAlive: boolean = true;

  /**
   * Server-side prepared statements on this connection, keyed by SQL text
   * (insertion order = LRU order). Prepared statements live per connection.
   */
  private readonly stmts = new Map<string, { name: string; nParams: number }>();
  private stmtSeq = 0;
  /** Capacity of {@link stmts}; the least recently used is CLOSE_STMT'd. */
  stmtCacheSize = DEFAULT_STATEMENT_CACHE_SIZE;

  private constructor(socket: net.Socket, metrics: Metrics, via: 'unix' | 'tcp') {
    this.socket = socket;
    this.metrics = metrics;
    this.via = via;

    this.attach(socket);
  }

  private attach(socket: net.Socket): void {
    socket.on('data', (chunk: Buffer) => {
      this.metrics.bytesRead += chunk.length;
      this.readBuf = Buffer.concat([this.readBuf, chunk]);
      this.tryParse();
    });

    socket.on('error', (err: Error) => {
      this.isAlive = false;
      this.metrics.connectionsErrored++;
      if (this.pendingReject) {
        const reject = this.pendingReject;
        this.pendingResolve = undefined;
        this.pendingReject = undefined;
        this.pendingStreamChunks = undefined;
        reject(new ConnectionError(err.message));
      }
    });

    socket.on('close', () => {
      this.isAlive = false;
      if (this.pendingReject) {
        const reject = this.pendingReject;
        this.pendingResolve = undefined;
        this.pendingReject = undefined;
        this.pendingStreamChunks = undefined;
        reject(new ConnectionError('Connection closed unexpectedly'));
      }
    });
  }

  /**
   * Establish a connection and send the HELLO handshake.
   * On localhost, tries Unix socket first; falls back to TCP.
   */
  static async connect(
    host: string,
    port: number,
    timeoutMs: number,
    metrics: Metrics,
    auth?: { user: string; password: string },
    transport: 'auto' | 'unix' | 'tcp' = 'auto',
    unixSocketDir: string = DEFAULT_UNIX_SOCKET_DIR,
    ssl?: boolean | tls.ConnectionOptions,
  ): Promise<Connection> {
    if (ssl && transport === 'unix') throw new ConnectionError('TLS requires TCP');
    const deadline = Date.now() + timeoutMs;
    const { socket, via } = await Connection.openSocket(host, port, timeoutMs, ssl ? 'tcp' : transport, unixSocketDir);
    socket.setNoDelay(true);
    socket.setTimeout(timeoutMs);

    const conn = new Connection(socket, metrics, via);
    metrics.connectionsCreated++;

    // Perform HELLO handshake, bounded by the timeout; a failed handshake must
    // not leak its socket.
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new ConnectionError(`HELLO timed out after ${timeoutMs}ms`)),
          Math.max(1, deadline - Date.now()),
        );
        conn.sendHello(Boolean(ssl)).then(async (caps) => {
          if (ssl) {
            if (!(caps & 0x08)) throw new ConnectionError('Server did not accept TLS; refusing plaintext');
            await conn.secure(host, ssl);
          }
          if (auth || (caps & 0x20)) await conn.sendAuth(auth?.user ?? 'particledb', auth?.password ?? '');
        },
        ).then(
          () => { clearTimeout(timer); resolve(); },
          (err) => { clearTimeout(timer); reject(err); },
        );
      });
    } catch (err) {
      conn.close();
      throw err;
    }

    return conn;
  }

  private static async openSocket(
    host: string,
    port: number,
    timeoutMs: number,
    transport: 'auto' | 'unix' | 'tcp' = 'auto',
    unixSocketDir: string = DEFAULT_UNIX_SOCKET_DIR,
  ): Promise<{ socket: net.Socket; via: 'unix' | 'tcp' }> {
    const eps = nativeEndpoints(host, port, transport, unixSocketDir);
    let lastErr: Error | undefined;
    for (const ep of eps) {
      try {
        const socket = await Connection.dial(ep, ep.fallback ? Math.min(timeoutMs, 2000) : timeoutMs);
        return { socket, via: ep.kind };
      } catch (err) {
        lastErr = err as Error;
        // The default socket exists but does not answer (stale file, or a
        // peer this user may not use): try TCP.
        if (!ep.fallback) break;
      }
    }
    throw lastErr ?? new ConnectionError('no endpoint to connect to');
  }

  private static dial(ep: NativeEndpoint, timeoutMs: number): Promise<net.Socket> {
    return new Promise<net.Socket>((resolve, reject) => {
      const what = ep.kind === 'unix' ? ep.path : `${ep.host}:${ep.port}`;
      const sock =
        ep.kind === 'unix'
          ? net.createConnection({ path: ep.path })
          : net.createConnection({ host: ep.host, port: ep.port });
      const timer = setTimeout(() => {
        sock.destroy(new ConnectionError(`Connection to ${what} timed out`));
      }, timeoutMs);
      sock.once('connect', () => {
        clearTimeout(timer);
        resolve(sock);
      });
      sock.once('error', (err) => {
        clearTimeout(timer);
        reject(err instanceof ConnectionError ? err : new ConnectionError(err.message));
      });
    });
  }

  /** Send the HELLO handshake frame and wait for HELLO_OK. */
  private async sendHello(secure: boolean): Promise<number> {
    const payload = Buffer.from([HELLO_VERSION, 1, 0x01 | 0x10 | (secure ? 0x08 : 0), 1, FMT_ARROW_IPC]);

    const frame = await this.sendAndReceive(MSG_HELLO, payload, 0);
    if (frame.msgType !== MSG_HELLO_OK) {
      throw new ConnectionError(
        `Expected HELLO_OK (0x80), got 0x${frame.msgType.toString(16)}`
      );
    }
    return frame.data.length >= 11 ? frame.data[1] : 0;
  }

  private async secure(host: string, options: boolean | tls.ConnectionOptions): Promise<void> {
    const plain = this.socket;
    plain.removeAllListeners('data');
    plain.removeAllListeners('error');
    plain.removeAllListeners('close');
    const name = host.replace(/^\[|\]$/g, '');
    const secure = tls.connect({
      minVersion: 'TLSv1.2',
      ...(typeof options === 'object' ? options : {}),
      socket: plain,
      servername: typeof options === 'object' && options.servername ? options.servername : net.isIP(name) ? undefined : name,
      checkServerIdentity: (_hostname, cert) => tls.checkServerIdentity(
        typeof options === 'object' && options.servername ? options.servername : name, cert),
    });
    this.socket = secure;
    this.attach(secure);
    await new Promise<void>((resolve, reject) => {
      secure.once('secureConnect', resolve);
      secure.once('error', reject);
      secure.once('close', () => reject(new ConnectionError('Connection closed during TLS handshake')));
    });
  }

  /**
   * Send MSG_AUTH `[u32 userlen][user][u32 passlen][pass]` and require MSG_OK.
   * A rejected login surfaces as the server's error (QueryError).
   */
  private async sendAuth(user: string, password: string): Promise<void> {
    const u = Buffer.from(user, 'utf-8');
    const p = Buffer.from(password, 'utf-8');
    const payload = Buffer.alloc(8 + u.length + p.length);
    payload.writeUInt32LE(u.length, 0);
    u.copy(payload, 4);
    payload.writeUInt32LE(p.length, 4 + u.length);
    p.copy(payload, 8 + u.length);
    const frame = await this.sendAndReceive(MSG_AUTH, payload, 0);
    if (frame.msgType !== MSG_OK) {
      throw new ConnectionError(`Expected OK for AUTH, got 0x${frame.msgType.toString(16)}`);
    }
  }

  /**
   * Return the server-side prepared statement for `sql` on this connection,
   * sending MSG_PREPARE on first use. Evicts (MSG_CLOSE_STMT) the least
   * recently used statement when the cache is full.
   */
  async ensurePrepared(sql: string): Promise<{ name: string; nParams: number }> {
    const hit = this.stmts.get(sql);
    if (hit) {
      this.stmts.delete(sql);
      this.stmts.set(sql, hit);
      return hit;
    }
    while (this.stmts.size >= Math.max(1, this.stmtCacheSize)) {
      const [oldSql, old] = this.stmts.entries().next().value as [string, { name: string }];
      this.stmts.delete(oldSql);
      await this.closeStatement(old.name);
    }
    const name = `pdb_ts_${++this.stmtSeq}`;
    const s = Buffer.from(sql, 'utf-8');
    const sqlLen = Buffer.alloc(4);
    sqlLen.writeUInt32LE(s.length, 0);
    const frame = await this.sendAndReceive(MSG_PREPARE, Buffer.concat([nameHeader(name), sqlLen, s]), 0);
    if (frame.msgType !== MSG_PREPARE_OK || frame.data.length < 2) {
      this.close();
      throw new PdbError(`protocol error: expected PREPARE_OK (0x89), got 0x${frame.msgType.toString(16)}`);
    }
    const entry = { name, nParams: frame.data.readUInt16LE(0) };
    this.stmts.set(sql, entry);
    return entry;
  }

  /** Forget the cached prepared statement for `sql` (after an EXECUTE error). */
  forgetPrepared(sql: string): void {
    this.stmts.delete(sql);
  }

  /** Send MSG_CLOSE_STMT for `name` and require MSG_OK. */
  async closeStatement(name: string): Promise<void> {
    const frame = await this.sendAndReceive(MSG_CLOSE_STMT, nameHeader(name), 0);
    if (frame.msgType !== MSG_OK) {
      this.close();
      throw new PdbError(`protocol error: expected OK for CLOSE_STMT, got 0x${frame.msgType.toString(16)}`);
    }
  }

  /**
   * Run `sql`: a plain MSG_QUERY when there are no parameters, otherwise the
   * cached prepared statement plus MSG_EXECUTE with a 1-row Arrow parameter
   * stream. Resolves with the reply frame (ARROW_RESULT or OK).
   */
  async run(sql: string, params?: any[]): Promise<PdbFrame> {
    if (!params || params.length === 0) return this.sendQuery(sql);
    const stmt = await this.ensurePrepared(sql);
    if (stmt.nParams !== params.length) {
      throw new QueryError(
        `statement expects ${stmt.nParams} parameter(s) but ${params.length} were supplied`,
      );
    }
    try {
      return await this.sendAndReceive(MSG_EXECUTE, Buffer.concat([nameHeader(stmt.name), encodeParamRow(params)]), 0);
    } catch (err) {
      if (err instanceof QueryError) this.forgetPrepared(sql);
      throw err;
    }
  }

  /**
   * Run `sql` once per row of `rows` with MSG_EXECUTE_BATCH (Arrow IPC
   * parameter stream, M rows x N columns), splitting into several frames when
   * one would exceed the server's payload cap. With `stopOnError` (inside a
   * transaction) no frame is sent after one that reported a failure.
   */
  async runBatch(
    sql: string,
    rows: any[][],
    types: ParamType[] | undefined,
    stopOnError: boolean,
  ): Promise<{ rowCounts: Array<number | null>; errors: BatchRowError[] }> {
    const stmt = await this.ensurePrepared(sql);
    for (let i = 0; i < rows.length; i++) {
      if (!Array.isArray(rows[i]) || rows[i].length !== stmt.nParams) {
        throw new QueryError(
          `executeBatch: row ${i} has ${Array.isArray(rows[i]) ? rows[i].length : 'no'} value(s); the statement expects ${stmt.nParams}`,
        );
      }
    }
    const rowCounts: Array<number | null> = new Array(rows.length).fill(null);
    const errors: BatchRowError[] = [];

    if (stmt.nParams === 0) {
      // A zero-parameter EXECUTE_BATCH carries no columns, hence no row count:
      // the server runs it ONCE whatever rows.length is. Run it once per row.
      for (let i = 0; i < rows.length; i++) {
        try {
          const frame = await this.sendQuery(sql);
          rowCounts[i] = frame.msgType === MSG_OK && frame.data.length >= 8 ? Number(frame.data.readBigUInt64LE(0)) : 0;
        } catch (err) {
          if (!(err instanceof QueryError)) throw err;
          errors.push({ row: i, message: err.message });
          if (stopOnError) break;
        }
      }
      return { rowCounts, errors };
    }

    const norm = rows.map((r) => r.map(normalizeParam));
    const colTypes = batchParamTypes(norm, stmt.nParams, types);
    const header = nameHeader(stmt.name);

    const encode = (from: number, to: number): Buffer =>
      encodeArrowStream(
        colTypes.map((type, c) => ({
          name: `p${c + 1}`,
          type,
          values: norm.slice(from, to).map((r) => r[c]),
        })),
      );

    // Encode EVERY frame before sending the first: outside a transaction each
    // sent frame commits, so a bad value or an oversized row found while
    // encoding frame k must be raised before frames 0..k-1 have committed.
    const plan: Array<{ from: number; to: number; payload: Buffer }> = [];
    for (let from = 0; from < rows.length; ) {
      let to = Math.min(rows.length, from + MAX_BATCH_ROWS_PER_FRAME);
      let payload = encode(from, to);
      while (header.length + payload.length > MAX_PAYLOAD_SIZE && to - from > 1) {
        to = from + Math.ceil((to - from) / 2);
        payload = encode(from, to);
      }
      if (header.length + payload.length > MAX_PAYLOAD_SIZE) {
        throw new RangeError(
          `executeBatch: row ${from} alone exceeds the ${MAX_PAYLOAD_SIZE}-byte frame limit; nothing was sent`,
        );
      }
      plan.push({ from, to, payload });
      from = to;
    }

    for (const { from, to, payload } of plan) {
      const frame = await this.sendAndReceive(MSG_EXECUTE_BATCH, Buffer.concat([header, payload]), 0);
      let entries: BatchEntry[];
      try {
        entries = decodeExecuteBatchResult(frame, to - from);
      } catch (err) {
        this.close(); // a malformed reply: the stream can no longer be trusted
        throw err;
      }
      entries.forEach((e, k) => {
        if (e.ok) rowCounts[from + k] = e.count;
        else errors.push({ row: from + k, message: e.message });
      });
      if (stopOnError && errors.length > 0) break;
    }
    return { rowCounts, errors };
  }

  /** Send a QUERY frame and wait for the response. */
  async sendQuery(sql: string, streamId: number = 0): Promise<PdbFrame> {
    const payload = Buffer.from(sql, 'utf-8');
    return this.sendAndReceive(MSG_QUERY, payload, streamId);
  }

  /** Send a BATCH frame and wait for BATCH_RESULT. */
  async sendBatch(sqls: string[], streamId: number = 0): Promise<PdbFrame> {
    const parts: Buffer[] = [];
    const countBuf = Buffer.alloc(4);
    countBuf.writeUInt32LE(sqls.length, 0);
    parts.push(countBuf);

    for (const sql of sqls) {
      const sqlBuf = Buffer.from(sql, 'utf-8');
      const lenBuf = Buffer.alloc(4);
      lenBuf.writeUInt32LE(sqlBuf.length, 0);
      parts.push(lenBuf, sqlBuf);
    }

    const payload = Buffer.concat(parts);
    const frame = await this.sendAndReceive(MSG_BATCH, payload, streamId);
    try {
      decodeBatchResult(frame, sqls.length);
    } catch (err) {
      // A malformed reply means the stream can no longer be trusted.
      if (!(err instanceof QueryError)) this.close();
      throw err;
    }
    return frame;
  }

  /** Send a PING and wait for PONG. */
  async ping(): Promise<boolean> {
    const frame = await this.sendAndReceive(MSG_PING, Buffer.alloc(0), 0);
    return frame.msgType === MSG_PONG;
  }

  /** Send a CLOSE notification and destroy the socket. */
  close(): void {
    if (!this.isAlive) return;
    this.isAlive = false;
    this.metrics.connectionsClosed++;

    const hdr = Connection.buildHeader(0, 0, MSG_CLOSE, 0);
    try { this.socket.write(hdr); } catch { /* ignore */ }
    this.socket.destroy();
  }

  // ── Private helpers ─────────────────────────────────────────────────

  private static buildHeader(
    payloadLen: number,
    streamId: number,
    msgType: number,
    flags: number
  ): Buffer {
    const hdr = Buffer.alloc(HEADER_SIZE);
    hdr.writeUInt32LE(payloadLen, 0);
    hdr.writeUInt16LE(streamId, 4);
    hdr[6] = msgType;
    hdr[7] = flags;
    return hdr;
  }

  private sendAndReceive(
    msgType: number,
    payload: Buffer,
    streamId: number
  ): Promise<PdbFrame> {
    return new Promise<PdbFrame>((resolve, reject) => {
      if (!this.isAlive) {
        reject(new ConnectionError('Connection is closed'));
        return;
      }

      if (this.pendingResolve) {
        reject(new ConnectionError('Connection is already handling a request'));
        return;
      }

      if (payload.length > MAX_PAYLOAD_SIZE) { reject(new QueryError('Request exceeds the 16 MiB frame limit')); return; }
      const hdr = Connection.buildHeader(payload.length, streamId, msgType, 0);
      const frame = Buffer.concat([hdr, payload]);

      this.metrics.bytesWritten += frame.length;
      this.pendingResolve = resolve;
      this.pendingReject = reject;
      this.pendingStreamChunks = undefined;

      this.socket.write(frame, (err) => {
        if (err) {
          this.pendingResolve = undefined;
          this.pendingReject = undefined;
          this.isAlive = false;
          reject(new ConnectionError(err.message));
        }
      });

      // tryParse in case data arrived before we set up pendingResolve
      this.tryParse();
    });
  }

  private tryParse(): void {
    while (this.readBuf.length >= HEADER_SIZE) {
      const payloadLen = this.readBuf.readUInt32LE(0);
      if (payloadLen > MAX_PAYLOAD_SIZE) {
        this.socket.destroy(new ConnectionError('Response exceeds the 16 MiB frame limit'));
        return;
      }
      const totalLen = HEADER_SIZE + payloadLen;
      if (this.readBuf.length < totalLen) break;

      const streamId = this.readBuf.readUInt16LE(4);
      const msgType = this.readBuf[6];
      const flags = this.readBuf[7];
      const data = Buffer.from(this.readBuf.subarray(HEADER_SIZE, totalLen));
      this.readBuf = this.readBuf.subarray(totalLen);

      this.handleFrame({ streamId, msgType, flags, data });
    }
  }

  private handleFrame(frame: PdbFrame): void {
    if (frame.msgType === MSG_ARROW_STREAM) {
      // Chunked Arrow IPC response
      if (!this.pendingStreamChunks) {
        this.pendingStreamChunks = [];
      }
      this.pendingStreamChunks.push(frame.data);

      const more = (frame.flags & FLAG_MORE_CHUNKS) !== 0;
      if (!more) {
        // Last chunk - combine and resolve as a synthetic ARROW_RESULT frame
        const combined = Buffer.concat(this.pendingStreamChunks);
        this.pendingStreamChunks = undefined;
        this.resolveFrame({
          streamId: frame.streamId,
          msgType: MSG_ARROW_RESULT,
          flags: 0,
          data: combined,
        });
      }
      return;
    }

    this.resolveFrame(frame);
  }

  private resolveFrame(frame: PdbFrame): void {
    const resolve = this.pendingResolve;
    const reject = this.pendingReject;
    this.pendingResolve = undefined;
    this.pendingReject = undefined;

    if (!resolve || !reject) return;

    if (frame.msgType === MSG_ERROR) {
      const errCode = frame.data.length >= 2 ? frame.data.readUInt16LE(0) : 0;
      const errMsg = frame.data.length > 2 ? frame.data.subarray(2).toString('utf-8') : 'Unknown error';

      if (errCode === 28) {
        reject(new DiskFullError(errMsg));
      } else {
        reject(new QueryError(errMsg, errCode, sqlstateOf(errCode, errMsg)));
      }
    } else {
      resolve(frame);
    }
  }
}

// ---------------------------------------------------------------------------
// Transaction context
// ---------------------------------------------------------------------------

/**
 * A transaction context obtained via `PdbWireClient.transaction()`.
 *
 * All queries issued through a TransactionContext run on the same connection
 * inside an explicit BEGIN/COMMIT/ROLLBACK block.
 */
export class TransactionContext {
  private active = true;
  private pending: Promise<void> = Promise.resolve();

  private run<T>(operation: () => Promise<T>): Promise<T> {
    if (!this.active) return Promise.reject(new QueryError('Transaction has finished'));
    const result = this.pending.then(() => new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.conn.close();
        reject(new ConnectionError(`Request timed out after ${this.timeoutMs}ms`));
      }, this.timeoutMs);
      operation().then(value => { clearTimeout(timer); resolve(value); },
        error => { clearTimeout(timer); reject(error); });
    }));
    this.pending = result.then(() => undefined, () => undefined);
    return result;
  }

  /** @internal */
  async finish(): Promise<void> { this.active = false; await this.pending; }
  /** @internal */
  constructor(
    private readonly conn: Connection,
    private readonly metricsRef: Metrics,
    private readonly timeoutMs: number,
  ) {}

  /**
   * Run a query and return parsed rows.
   *
   * @param sql - SQL query string; `$1, $2, ...` placeholders bind `params`.
   * @param params - Optional parameter values (sent via PREPARE + EXECUTE).
   * @returns Array of result rows.
   */
  async query<T extends Row = Row>(sql: string, params?: any[]): Promise<T[]> {
    return (await this.queryResult<T>(sql, params)).rows;
  }

  /**
   * Run a statement and return rows, row count and column names.
   */
  async queryResult<T = Row>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    this.metricsRef.queriesTotal++;
    const frame = await this.run(() => this.conn.run(sql, params));
    const result = frameToResult<T>(frame);
    this.metricsRef.rowsReceived += result.rows.length;
    return result;
  }

  /** Run a statement and return positional rows with column descriptions. */
  async queryArrays(sql: string, params?: any[], options?: DecodeOptions): Promise<RawQueryResult> {
    this.metricsRef.queriesTotal++;
    const result = frameToRaw(await this.run(() => this.conn.run(sql, params)), options);
    this.metricsRef.rowsReceived += result.rows.length;
    return result;
  }

  /**
   * Run a DML statement and return affected row count.
   *
   * @param sql - SQL statement; `$1, $2, ...` placeholders bind `params`.
   * @param params - Optional parameter values.
   * @returns Number of rows affected.
   */
  async execute(sql: string, params?: any[]): Promise<number> {
    this.metricsRef.queriesTotal++;
    const frame = await this.run(() => this.conn.run(sql, params));
    if (frame.msgType === MSG_OK && frame.data.length >= 8) {
      return Number(frame.data.readBigUInt64LE(0));
    }
    return 0;
  }

  /**
   * Run `sql` once per parameter set with MSG_EXECUTE_BATCH inside this
   * transaction. The first failing set aborts the transaction: this rejects
   * with {@link BatchExecuteError} and `transaction()` rolls back.
   */
  async executeBatch(sql: string, rows: any[][], options?: { types?: ParamType[] }): Promise<BatchExecuteResult> {
    this.metricsRef.batchesTotal++;
    const { rowCounts, errors } = await this.run(() => this.conn.runBatch(sql, rows, options?.types, true));
    if (errors.length > 0) throw new BatchExecuteError(rowCounts, errors);
    const counts = rowCounts as number[];
    return { rowCounts: counts, totalRowCount: counts.reduce((a, b) => a + b, 0) };
  }
}

// ---------------------------------------------------------------------------
// Reserved connection
// ---------------------------------------------------------------------------

/** Leading keyword of a statement, upper-cased (comments and blanks skipped). */
function leadingKeywords(sql: string): string {
  return sql.replace(/^(\s+|--[^\n]*\n?|\/\*[\s\S]*?\*\/)*/, '').slice(0, 32).toUpperCase();
}

/**
 * One pooled connection checked out by {@link PdbWireClient.reserve} for
 * exclusive use: the same server session for every statement until
 * `release()`. Statements run in the order they are issued, even when
 * started concurrently.
 *
 * The v1 wire reports no transaction status, so the connection tracks it from
 * the statements it runs (`BEGIN` / `START TRANSACTION` open a block;
 * `COMMIT` / `END` / `ROLLBACK` / `ABORT` close it). `release()` rolls back a
 * block that is still open, and closes the socket instead of pooling it when
 * that fails or a request's outcome is unknown.
 */
export class ReservedConnection {
  private pending: Promise<void> = Promise.resolve();
  private released = false;
  private maybeInBlock = false;

  /** @internal */
  constructor(
    private readonly conn: Connection,
    private readonly metricsRef: Metrics,
    private readonly timeoutMs: number,
    private readonly giveBack: (conn: Connection) => void,
  ) {}

  /** True once `release()` was called. */
  get isReleased(): boolean { return this.released; }

  /** True while the connection may be inside a transaction block. */
  get inTransaction(): boolean { return this.maybeInBlock; }

  private run(sql: string, params?: any[]): Promise<PdbFrame> {
    if (this.released) return Promise.reject(new PdbError('Reserved connection was released'));
    const op = this.pending.then(() => new Promise<PdbFrame>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.conn.close();
        reject(new ConnectionError(`Request timed out after ${this.timeoutMs}ms`));
      }, this.timeoutMs);
      this.conn.run(sql, params).then(
        (f) => { clearTimeout(timer); resolve(f); },
        (e) => {
          clearTimeout(timer);
          if (e instanceof ConnectionError) this.conn.close();
          reject(e);
        },
      );
    }));
    this.pending = op.then(() => undefined, () => undefined);
    return op.then((frame) => {
      const kw = leadingKeywords(sql);
      if (/^(BEGIN|START\s+TRANSACTION)\b/.test(kw)) this.maybeInBlock = true;
      else if (/^(COMMIT|END|ABORT)\b/.test(kw) || (/^ROLLBACK\b/.test(kw) && !/^ROLLBACK\s+TO\b/.test(kw))) {
        this.maybeInBlock = false;
      }
      return frame;
    });
  }

  /** Run a statement and return positional rows with column descriptions. */
  async queryArrays(sql: string, params?: any[], options?: DecodeOptions): Promise<RawQueryResult> {
    this.metricsRef.queriesTotal++;
    const result = frameToRaw(await this.run(sql, params), options);
    this.metricsRef.rowsReceived += result.rows.length;
    return result;
  }

  /** Run a statement and return row objects, row count and column names. */
  async query<T = Row>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    this.metricsRef.queriesTotal++;
    return frameToResult<T>(await this.run(sql, params));
  }

  /** Run a statement and return the number of rows affected. */
  async execute(sql: string, params?: any[]): Promise<number> {
    return (await this.query(sql, params)).rowCount;
  }

  /**
   * Return the connection to the pool. A transaction block left open is
   * rolled back first; if that fails, or the connection was lost, the socket
   * is closed instead of pooled. Idempotent.
   */
  async release(): Promise<void> {
    if (this.released) return;
    await this.pending;
    if (this.maybeInBlock && this.conn.isAlive) {
      try {
        await this.run('ROLLBACK');
      } catch {
        this.conn.close();
      }
    }
    this.released = true;
    this.giveBack(this.conn);
  }

  /** Close the socket instead of pooling it, then release. */
  async destroy(): Promise<void> {
    if (this.released) return;
    this.conn.close();
    this.released = true;
    this.giveBack(this.conn);
  }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

/**
 * A simple async connection pool.
 * @internal
 */
class ConnectionPool extends EventEmitter {
  private idle: Connection[] = [];
  private active: number = 0;
  private waitQueue: Array<{ resolve: (conn: Connection) => void; reject: (error: Error) => void }> = [];
  private closed = false;

  constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly maxSize: number,
    private readonly timeoutMs: number,
    private readonly metrics: Metrics,
    private readonly auth?: { user: string; password: string },
    private readonly transport: 'auto' | 'unix' | 'tcp' = 'auto',
    private readonly stmtCacheSize: number = DEFAULT_STATEMENT_CACHE_SIZE,
    private readonly unixSocketDir: string = DEFAULT_UNIX_SOCKET_DIR,
    private readonly ssl?: boolean | tls.ConnectionOptions,
  ) {
    super();
  }

  toJSON(): object { return {host: this.host, port: this.port, maxSize: this.maxSize, active: this.active, idle: this.idle.length}; }
  [inspect.custom](): object { return this.toJSON(); }

  /** Open a new physical connection with this pool's settings. */
  private async open(): Promise<Connection> {
    const conn = await Connection.connect(
      this.host, this.port, this.timeoutMs, this.metrics, this.auth, this.transport,
      this.unixSocketDir, this.ssl,
    );
    conn.stmtCacheSize = this.stmtCacheSize;
    return conn;
  }

  get idleCount(): number { return this.idle.length; }
  get activeCount(): number { return this.active; }

  async acquire(): Promise<Connection> {
    if (this.closed) throw new ConnectionError('Client is closed');
    // Return an idle connection if available
    while (this.idle.length > 0) {
      const conn = this.idle.pop()!;
      if (conn.isAlive) {
        this.active++;
        return conn;
      }
      // Dead connection - discard and try next
    }

    // Create a new connection if under pool limit
    if (this.active < this.maxSize) {
      this.active++;
      try {
        const conn = await this.open();
        if (this.closed) { conn.close(); throw new ConnectionError('Client is closed'); }
        return conn;
      } catch (err) {
        this.active--;
        // Nothing of the caller's request was written: safe to retry.
        throw new ConnectFailedError((err as Error).message);
      }
    }

    // Wait for a connection to be released
    this.metrics.poolWaits++;
    return new Promise<Connection>((resolve, reject) => {
      let settled = false;
      const waiter = {
        resolve: (conn: Connection) => {
          if (settled) { this.release(conn); return; }
          settled = true; clearTimeout(timer); resolve(conn);
        },
        reject: (err: Error) => {
          if (settled) return;
          settled = true; clearTimeout(timer); reject(err);
        },
      };
      const timer = setTimeout(() => {
        const index = this.waitQueue.indexOf(waiter);
        if (index !== -1) this.waitQueue.splice(index, 1);
        waiter.reject(new ConnectionError('Pool wait timeout: no connection available'));
      }, this.timeoutMs);
      this.waitQueue.push(waiter);
    });
  }

  release(conn: Connection): void {
    if (this.closed) { conn.close(); this.active = Math.max(0, this.active - 1); return; }
    if (!conn.isAlive) {
      this.active = Math.max(0, this.active - 1);
      this.dispatchWaiter();
      return;
    }

    if (this.waitQueue.length > 0) {
      // Hand directly to a waiter
      const waiter = this.waitQueue.shift()!;
      waiter.resolve(conn);
      return;
    }

    // Return to idle pool
    this.active = Math.max(0, this.active - 1);
    if (this.idle.length < this.maxSize) {
      this.idle.push(conn);
    } else {
      conn.close();
    }
  }

  private dispatchWaiter(): void {
    if (this.waitQueue.length === 0) return;
    // Waiter will create a new connection when it goes through acquire()
    // Wake one waiter by handing them a fresh connection attempt
    const waiter = this.waitQueue.shift()!;
    this.active++;
    this.open()
      .then((conn) => {
        if (this.closed) { conn.close(); this.active--; waiter.reject(new ConnectionError('Client is closed')); }
        else waiter.resolve(conn);
      })
      .catch((err) => {
        this.active = Math.max(0, this.active - 1);
        waiter.reject(new ConnectFailedError((err as Error).message));
        this.dispatchWaiter();
      });
  }

  closeAll(): void {
    this.closed = true;
    for (const waiter of this.waitQueue.splice(0)) waiter.reject(new ConnectionError('Client is closed'));
    for (const conn of this.idle) {
      conn.close();
    }
    this.idle = [];
  }
}

// ---------------------------------------------------------------------------
// PdbWireClient - public API
// ---------------------------------------------------------------------------

/**
 * PDB native wire protocol client with async connection pooling.
 *
 * Defaults to Unix socket `/tmp/.s.PDB.{port}` when connecting to localhost,
 * and falls back to TCP automatically. For remote hosts, plain TCP is used.
 *
 * All methods are fully async and safe to call concurrently.
 *
 * @example
 * ```ts
 * import { PdbWireClient } from '@particledb/sdk';
 *
 * const db = new PdbWireClient({ host: 'localhost', port: 7173, poolSize: 8 });
 *
 * const rows = await db.query('SELECT * FROM users');
 * console.log(rows); // [{ id: 1, name: 'alice' }, ...]
 *
 * await db.close();
 * ```
 */
export class PdbWireClient {
  private readonly pool: ConnectionPool;
  private readonly opts: Required<Omit<ConnectionOptions, 'user' | 'password' | 'ssl'>>;
  private readonly metricsData: Metrics;
  private closed: boolean = false;

  constructor(opts?: ConnectionOptions) {
    this.opts = {
      host: opts?.host ?? 'localhost',
      port: opts?.port ?? DEFAULT_PORT,
      poolSize: opts?.poolSize ?? DEFAULT_POOL_SIZE,
      timeoutMs: opts?.timeoutMs ?? DEFAULT_TIMEOUT_MS,
      transport: opts?.transport ?? 'auto',
      unixSocketDir: opts?.unixSocketDir ?? DEFAULT_UNIX_SOCKET_DIR,
      metricsEnabled: opts?.metricsEnabled ?? true,
      maxRetries: opts?.maxRetries ?? DEFAULT_MAX_RETRIES,
      retryDelayMs: opts?.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS,
      statementCacheSize: opts?.statementCacheSize ?? DEFAULT_STATEMENT_CACHE_SIZE,
    };

    for (const [key, value] of Object.entries({port: this.opts.port, poolSize: this.opts.poolSize, timeoutMs: this.opts.timeoutMs, statementCacheSize: this.opts.statementCacheSize})) {
      if (!Number.isSafeInteger(value) || value <= 0 || (key === 'port' && value > 65535)) throw new PdbError(`Invalid ${key}: expected a positive integer`);
    }
    if (opts?.ssl && this.opts.transport === 'unix') throw new PdbError('TLS requires TCP');
    this.metricsData = createMetrics();

    // MSG_AUTH is sent only when a password is configured; a server started
    // with --no-auth needs none.
    const auth =
      opts?.password !== undefined || opts?.user !== undefined
        ? { user: opts.user ?? 'particledb', password: opts.password ?? '' }
        : undefined;

    this.pool = new ConnectionPool(
      this.opts.host,
      this.opts.port,
      this.opts.poolSize,
      this.opts.timeoutMs,
      this.metricsData,
      auth,
      this.opts.transport,
      this.opts.statementCacheSize,
      this.opts.unixSocketDir, opts?.ssl,
    );
  }

  // ── Public API ──────────────────────────────────────────────────────────

  /**
   * Execute a query and return typed rows.
   *
   * Arrow IPC responses are decoded by the built-in decoder into plain `Row`
   * objects (int64 → number, or bigint beyond 2^53; decimal → string;
   * timestamp → Date; date → 'YYYY-MM-DD').
   *
   * @param sql - SQL query string; `$1, $2, ...` placeholders bind `params`.
   * @param params - Optional parameter values. When given, the statement is
   *   prepared once per connection (MSG_PREPARE, cached) and run with
   *   MSG_EXECUTE carrying the values as a 1-row Arrow batch.
   * @returns Array of result rows.
   *
   * @example
   * ```ts
   * const users = await db.query('SELECT id, name FROM users WHERE id = $1', [42]);
   * ```
   */
  async query<T extends Row = Row>(sql: string, params?: any[]): Promise<T[]> {
    return (await this.queryResult<T>(sql, params)).rows;
  }

  /**
   * Execute a statement and return rows, row count (rows returned, or rows
   * affected for DML) and column names.
   */
  async queryResult<T = Row>(sql: string, params?: any[]): Promise<QueryResult<T>> {
    this.assertOpen();
    this.metricsData.queriesTotal++;
    const start = Date.now();
    const frame = await this.sendOne(sql, params);
    this.metricsData.latencySumMs += Date.now() - start;
    const result = frameToResult<T>(frame);
    this.metricsData.rowsReceived += result.rows.length;
    return result;
  }

  /**
   * Execute a statement and return positional rows with the server's column
   * descriptions (Arrow type and declared SQL type per column). This is the
   * entry point for driver layers such as ORM adapters.
   */
  async queryArrays(sql: string, params?: any[], options?: DecodeOptions): Promise<RawQueryResult> {
    this.assertOpen();
    this.metricsData.queriesTotal++;
    const result = frameToRaw(await this.sendOne(sql, params), options);
    this.metricsData.rowsReceived += result.rows.length;
    return result;
  }

  /**
   * Check one connection out of the pool for exclusive use until
   * `release()`: every statement on it runs on the same server session, so
   * the caller can drive `BEGIN` / `COMMIT` / `SAVEPOINT` itself (what ORM
   * transaction APIs do). Statements on a reserved connection run in the
   * order they are issued. Prefer {@link transaction} in application code.
   *
   * @example
   * ```ts
   * const conn = await client.reserve();
   * try {
   *   await conn.query('BEGIN');
   *   await conn.query('UPDATE t SET n = n + 1 WHERE id = $1', [1]);
   *   await conn.query('COMMIT');
   * } finally {
   *   await conn.release();
   * }
   * ```
   */
  async reserve(): Promise<ReservedConnection> {
    this.assertOpen();
    const conn = await this.acquireWithRetry();
    return new ReservedConnection(conn, this.metricsData, this.opts.timeoutMs, (c) => this.pool.release(c));
  }

  /**
   * Execute a DML statement (INSERT, UPDATE, DELETE, DDL) and return
   * the number of rows affected.
   *
   * @param sql - SQL statement; `$1, $2, ...` placeholders bind `params`.
   * @param params - Optional parameter values (PREPARE + EXECUTE).
   * @returns Number of rows affected.
   *
   * @example
   * ```ts
   * const n = await db.execute('UPDATE users SET active = $1 WHERE id = $2', [true, 1]);
   * ```
   */
  async execute(sql: string, params?: any[]): Promise<number> {
    this.assertOpen();
    this.metricsData.queriesTotal++;
    const start = Date.now();
    const frame = await this.sendOne(sql, params);
    this.metricsData.latencySumMs += Date.now() - start;
    if (frame.msgType === MSG_OK && frame.data.length >= 8) {
      return Number(frame.data.readBigUInt64LE(0));
    }
    return 0;
  }

  /**
   * Run one parameterized statement once per parameter set in a single
   * round-trip: MSG_PREPARE (cached) + MSG_EXECUTE_BATCH whose payload is an
   * Arrow IPC stream of `rows.length` rows x N parameter columns.
   *
   * Column types are inferred over all rows (integer/bigint → Int64, any
   * fractional number → Float64, string → Utf8, boolean → Boolean,
   * Buffer/Uint8Array → Binary, null/undefined → null) or given explicitly
   * with `options.types`.
   *
   * Outside a transaction every parameter set commits on its own. If any set
   * fails this rejects with {@link BatchExecuteError}; its `rowCounts` shows
   * which sets committed and `errors` lists the failures.
   *
   * @example
   * ```ts
   * const r = await db.executeBatch('INSERT INTO t (id, name) VALUES ($1, $2)',
   *   [[1, 'a'], [2, 'b']]);
   * r.totalRowCount; // 2
   * ```
   */
  async executeBatch(
    sql: string,
    rows: any[][],
    options?: { types?: ParamType[] },
  ): Promise<BatchExecuteResult> {
    this.assertOpen();
    this.metricsData.batchesTotal++;
    if (rows.length === 0) return { rowCounts: [], totalRowCount: 0 };
    const conn = await this.acquireWithRetry();
    let out: { rowCounts: Array<number | null>; errors: BatchRowError[] };
    try {
      out = await this.withTimeout(conn.runBatch(sql, rows, options?.types, false), this.opts.timeoutMs);
    } catch (err) {
      this.metricsData.queriesErrored++;
      if (isClientSideError(err)) {
        this.pool.release(conn);
        throw err;
      }
      if (err instanceof PdbError && !(err instanceof ConnectionError)) {
        await this.rollbackOrDiscard(conn);
        this.pool.release(conn);
        throw err;
      }
      const amb = this.ambiguous(conn, err);
      this.pool.release(conn);
      throw amb;
    }
    if (out.errors.length > 0) {
      // A raw BEGIN sent earlier on this pooled connection would leave it in
      // an aborted transaction: roll back (a no-op otherwise) or discard.
      await this.rollbackOrDiscard(conn);
      this.pool.release(conn);
      throw new BatchExecuteError(out.rowCounts, out.errors);
    }
    this.pool.release(conn);
    const counts = out.rowCounts as number[];
    return { rowCounts: counts, totalRowCount: counts.reduce((x, y) => x + y, 0) };
  }

  /**
   * Send multiple SQL statements in a single round-trip as a BATCH frame.
   *
   * This is more efficient than calling `execute()` repeatedly when you
   * have a group of independent statements.
   *
   * @param sqls - Array of SQL statement strings.
   *
   * @example
   * ```ts
   * await db.batch([
   *   'CREATE TABLE t (id INT)',
   *   'INSERT INTO t VALUES (1)',
   *   'INSERT INTO t VALUES (2)',
   * ]);
   * ```
   */
  async batch(sqls: string[]): Promise<void> {
    this.assertOpen();
    this.metricsData.batchesTotal++;
    const conn = await this.acquireWithRetry();
    try {
      await this.withTimeout(conn.sendBatch(sqls), this.opts.timeoutMs);
    } catch (err) {
      if (err instanceof QueryError) {
        // A failed statement stops the batch; if the batch opened a
        // transaction the connection is now inside a failed one. Roll it back
        // (a no-op outside a transaction) or discard the connection.
        await this.rollbackOrDiscard(conn);
        throw err;
      }
      throw this.ambiguous(conn, err);
    } finally {
      this.pool.release(conn);
    }
  }

  /**
   * Execute a function inside a database transaction.
   *
   * Sends `BEGIN` and `COMMIT` around the user function. If the function
   * throws, `ROLLBACK` is sent automatically and the error is re-thrown.
   *
   * The entire transaction runs on a single pooled connection.
   *
   * @param fn - Async function receiving a TransactionContext.
   * @returns The return value of `fn`.
   *
   * @example
   * ```ts
   * await db.transaction(async (tx) => {
   *   await tx.execute("INSERT INTO accounts VALUES (1, 1000)");
   *   await tx.execute("UPDATE totals SET sum = sum + 1000");
   * });
   * ```
   */
  async transaction<T>(fn: (tx: TransactionContext) => Promise<T>): Promise<T> {
    this.assertOpen();
    const conn = await this.acquireWithRetry();
    const ctx = new TransactionContext(conn, this.metricsData, this.opts.timeoutMs);
    try {
      await this.withTimeout(conn.sendBatch(['BEGIN']), this.opts.timeoutMs);
      const result = await fn(ctx);
      await ctx.finish();
      const commit = await this.withTimeout(conn.sendBatch(['COMMIT']), this.opts.timeoutMs);
      if (commit.flags & FLAG_TXN_ROLLED_BACK) {
        // The block had failed (a statement error fn caught and ignored): the
        // server answered COMMIT by rolling it back. Nothing was committed.
        throw new TransactionRolledBackError();
      }
      return result;
    } catch (err) {
      await ctx.finish();
      if (err instanceof ConnectionError) {
        // A transport failure or timeout (BEGIN, a statement, or COMMIT):
        // the transaction may even have committed. Never resent; the socket
        // is closed, not rolled back on, and not pooled.
        throw this.ambiguous(conn, err);
      }
      // Never hand a connection that may be inside a failed transaction back
      // to the pool: roll back on it, or close it if that fails.
      await this.rollbackOrDiscard(conn);
      throw err;
    } finally {
      await ctx.finish();
      this.pool.release(conn);
    }
  }

  /** Send ROLLBACK on `conn`; close the connection if that does not succeed. */
  private async rollbackOrDiscard(conn: Connection): Promise<void> {
    if (!conn.isAlive) return;
    try {
      await this.withTimeout(conn.sendBatch(['ROLLBACK']), this.opts.timeoutMs);
    } catch {
      conn.close();
    }
  }

  /**
   * Check whether the server is reachable by sending a PING frame.
   *
   * @returns `true` if the server responds with PONG, `false` otherwise.
   */
  async ping(): Promise<boolean> {
    if (this.closed) return false;
    let conn: Connection;
    try {
      conn = await this.pool.acquire();
    } catch {
      return false;
    }
    try {
      const ok = await this.withTimeout(conn.ping(), this.opts.timeoutMs);
      this.pool.release(conn);
      return ok;
    } catch {
      conn.close();
      this.pool.release(conn);
      return false;
    }
  }

  /**
   * Perform a health check and return connection status with latency.
   *
   * @returns A HealthResult describing the current state of the client.
   *
   * @example
   * ```ts
   * const h = await db.health();
   * console.log(h.connected, h.latencyMs);
   * ```
   */
  async health(): Promise<HealthResult> {
    if (this.closed) {
      return {
        connected: false,
        latencyMs: 0,
        protocol: 'pdb_wire',
        poolActive: 0,
        poolIdle: 0,
      };
    }

    const start = Date.now();
    try {
      const conn = await this.pool.acquire();
      let ok: boolean;
      try {
        ok = await this.withTimeout(conn.ping(), this.opts.timeoutMs);
      } catch (err) {
        conn.close(); // a late PONG may still arrive: never pool it
        this.pool.release(conn);
        throw err;
      }
      const via = conn.via;
      this.pool.release(conn);
      const latencyMs = Date.now() - start;
      return {
        connected: ok,
        latencyMs,
        protocol: 'pdb_wire',
        transport: via,
        serverVersion: HELLO_VERSION,
        poolActive: this.pool.activeCount,
        poolIdle: this.pool.idleCount,
      };
    } catch {
      return {
        connected: false,
        latencyMs: Date.now() - start,
        protocol: 'pdb_wire',
        poolActive: this.pool.activeCount,
        poolIdle: this.pool.idleCount,
      };
    }
  }

  /**
   * Return all 12 client-side metrics in Prometheus text exposition format.
   *
   * @returns Prometheus-formatted string with metric definitions and values.
   *
   * @example
   * ```ts
   * // Expose via HTTP endpoint
   * app.get('/metrics', (req, res) => {
   *   res.set('Content-Type', 'text/plain; version=0.0.4');
   *   res.send(db.metrics());
   * });
   * ```
   */
  metrics(): string {
    return formatPrometheus(this.metricsData);
  }

  /**
   * Get raw metrics data as a plain object.
   *
   * @returns A snapshot of all 12 metric counters.
   */
  metricsSnapshot(): Readonly<Metrics> {
    return { ...this.metricsData };
  }

  /**
   * Close all pooled connections and shut down the client.
   *
   * After calling `close()`, any further method calls will throw.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.pool.closeAll();
  }

  // ── Private helpers ────────────────────────────────────────────────────

  private assertOpen(): void {
    if (this.closed) {
      throw new ConnectionError('PdbWireClient is closed');
    }
  }

  /**
   * Wrap a promise with a timeout. Rejects with ConnectionError on expiry.
   */
  private withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new ConnectionError(`Request timed out after ${ms}ms`));
      }, ms);

      promise.then(
        (val) => { clearTimeout(timer); resolve(val); },
        (err) => { clearTimeout(timer); reject(err); }
      );
    });
  }

  /**
   * Send one QUERY frame. Only the pre-send phase (acquiring / opening a
   * connection) is retried. A server error answer leaves the connection in
   * sync and it is pooled again; any other failure is ambiguous (see
   * {@link ambiguous}).
   */
  private async sendOne(sql: string, params?: any[]): Promise<PdbFrame> {
    const conn = await this.acquireWithRetry();
    let frame: PdbFrame;
    try {
      frame = await this.withTimeout(conn.run(sql, params), this.opts.timeoutMs);
    } catch (err) {
      this.metricsData.queriesErrored++;
      if (isClientSideError(err)) {
        // Rejected before anything was sent (bad parameter value): in sync.
        this.pool.release(conn);
        throw err;
      }
      if (err instanceof QueryError || err instanceof DiskFullError || (err instanceof PdbError && !(err instanceof ConnectionError))) {
        // The stream is in sync, but a statement error may leave the connection inside a FAILED transaction (the caller may have sent BEGIN through this single-statement API), so it is discarded, never pooled.
        conn.close();
        this.pool.release(conn);
        throw err;
      }
      const amb = this.ambiguous(conn, err);
      this.pool.release(conn);
      throw amb;
    }
    this.pool.release(conn);
    return frame;
  }

  /**
   * Close `conn` after a transport failure or client-side timeout that
   * happened once its request had started to be written, and return the
   * error to throw. The server may have executed -- and committed -- the
   * request, so it is never resent; the socket may still deliver the late
   * reply (or sit inside a transaction), so it is closed and the caller's
   * `pool.release` drops it instead of pooling it.
   */
  private ambiguous(conn: Connection, err: unknown): ConnectionError {
    conn.close();
    const msg = err instanceof Error ? err.message : String(err);
    return new ConnectionError(
      `connection lost after the request was sent; it may or may not have been executed (not retried): ${msg}`,
    );
  }

  /**
   * Acquire a pooled connection, retrying ONLY a failure to open a new one
   * (connect refused / timed out, HELLO dropped) with exponential backoff:
   * that happens before any byte of the caller's request is written, so a
   * retry cannot execute it twice. `maxRetries` / `retryDelayMs` apply here
   * and nowhere else.
   */
  private async acquireWithRetry(): Promise<Connection> {
    const { maxRetries, retryDelayMs } = this.opts;
    let lastErr: Error = new PdbError('Unknown error');
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await this.pool.acquire();
      } catch (err) {
        if (!(err instanceof ConnectFailedError)) throw err;
        lastErr = err;
        if (attempt < maxRetries) {
          this.metricsData.queriesRetried++;
          await sleep(retryDelayMs * Math.pow(2, attempt));
        }
      }
    }
    throw new ConnectionError(`All ${maxRetries + 1} connection attempts failed: ${lastErr.message}`);
  }
}

/** A parameter rejected by the client-side encoder before any byte was sent. */
function isClientSideError(err: unknown): boolean {
  return err instanceof TypeError || err instanceof RangeError;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
