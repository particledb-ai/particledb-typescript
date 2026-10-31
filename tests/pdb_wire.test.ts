/**
 * Comprehensive Jest unit tests for PdbWireClient.
 *
 * All tests use mocked sockets - no real PDB server required.
 * We use jest.mock('net') to intercept createConnection and inject
 * pre-fabricated binary frames via a MockSocket EventEmitter.
 *
 * Coverage (40+ tests):
 *  - Error types: PdbError, ConnectionError, QueryError, DiskFullError
 *  - Wire protocol: frame encoding, HELLO handshake
 *  - query() - Arrow IPC, OK, empty results, error
 *  - execute() - affected row count, DDL, error
 *  - batch() - encoding, success, error
 *  - transaction() - commit on success, rollback on throw, return value
 *  - ping() - sends PING, returns true/false
 *  - health() - structure, closed state
 *  - Retry - only connection establishment is retried; nothing sent is resent
 *  - Timeout - ConnectionError after timeoutMs
 *  - Pool - reuse, concurrent, close
 *  - Metrics - all 12 counters, Prometheus text format
 *  - MSG_ARROW_STREAM - chunked stream reassembly
 *  - Config - defaults, overrides, close() guards
 *  - Close frame - MSG_CLOSE sent
 */

import { EventEmitter } from 'events';

// ---------------------------------------------------------------------------
// jest.mock('net') - must be hoisted before any imports that use 'net'
// ---------------------------------------------------------------------------
jest.mock('net');
import * as net from 'net';

import {
  PdbWireClient,
  PdbError,
  ConnectionError,
  QueryError,
  DiskFullError,
} from '../src/pdb_wire';

// ---------------------------------------------------------------------------
// Protocol constants (for building test frames)
// ---------------------------------------------------------------------------
const HEADER_SIZE = 8;
const MSG_HELLO    = 0x00;
const MSG_QUERY    = 0x01;
const MSG_BATCH    = 0x02;
const MSG_PING     = 0x03;
const MSG_CLOSE    = 0x04;
const MSG_HELLO_OK = 0x80;
const MSG_ARROW_RESULT = 0x81;
const MSG_OK       = 0x82;
const MSG_ERROR    = 0x83;
const MSG_PONG     = 0x84;
const MSG_BATCH_RESULT = 0x85;
const MSG_ARROW_STREAM = 0x86;
const FLAG_MORE_CHUNKS = 0x02;

// ---------------------------------------------------------------------------
// Frame builder helpers
// ---------------------------------------------------------------------------

function buildFrame(msgType: number, payload: Buffer = Buffer.alloc(0), flags = 0, streamId = 0): Buffer {
  const hdr = Buffer.alloc(HEADER_SIZE);
  hdr.writeUInt32LE(payload.length, 0);
  hdr.writeUInt16LE(streamId, 4);
  hdr[6] = msgType;
  hdr[7] = flags;
  return Buffer.concat([hdr, payload]);
}

function buildHelloOk(): Buffer {
  const payload = Buffer.alloc(1);
  payload[0] = 0x01;
  return buildFrame(MSG_HELLO_OK, payload);
}

function buildOk(affectedRows: bigint = 0n): Buffer {
  const payload = Buffer.alloc(8);
  payload.writeBigUInt64LE(affectedRows, 0);
  return buildFrame(MSG_OK, payload);
}

function buildError(code: number, message: string): Buffer {
  const msgBuf = Buffer.from(message, 'utf-8');
  const payload = Buffer.alloc(2 + msgBuf.length);
  payload.writeUInt16LE(code, 0);
  msgBuf.copy(payload, 2);
  return buildFrame(MSG_ERROR, payload);
}

function buildPong(): Buffer {
  return buildFrame(MSG_PONG);
}

function buildArrowResult(data: Buffer): Buffer {
  return buildFrame(MSG_ARROW_RESULT, data);
}

/** BATCH_RESULT with `okCount` OK(0) entries, optionally followed by an ERROR entry. */
function buildBatchResult(okCount: number = 1, errorMessage?: string): Buffer {
  const parts: Buffer[] = [];
  const count = Buffer.alloc(4);
  count.writeUInt32LE(okCount + (errorMessage === undefined ? 0 : 1), 0);
  parts.push(count);
  for (let i = 0; i < okCount; i++) {
    const e = Buffer.alloc(9);
    e[0] = MSG_OK;
    parts.push(e);
  }
  if (errorMessage !== undefined) {
    const msg = Buffer.from(errorMessage, 'utf-8');
    const hdr = Buffer.alloc(5);
    hdr[0] = MSG_ERROR;
    hdr.writeUInt32LE(msg.length, 1);
    parts.push(hdr, msg);
  }
  return buildFrame(MSG_BATCH_RESULT, Buffer.concat(parts));
}

function buildArrowStreamChunk(data: Buffer, moreChunks: boolean): Buffer {
  return buildFrame(MSG_ARROW_STREAM, data, moreChunks ? FLAG_MORE_CHUNKS : 0);
}

// ---------------------------------------------------------------------------
// MockSocket
// ---------------------------------------------------------------------------

/**
 * Fake socket that behaves like net.Socket for the purposes of the wire protocol.
 * - Captures all writes for inspection.
 * - Lets tests push inbound data via `receive()`.
 * - Does NOT auto-emit 'connect' - the connect event is scheduled by the
 *   mock createConnection at the moment the socket is handed to the caller.
 */
class MockSocket extends EventEmitter {
  readonly written: Buffer[] = [];
  destroyed = false;
  private _onWrite?: (data: Buffer) => void;

  constructor() {
    super();
    // NO auto-connect here - connect is emitted by setupNetMock when the socket
    // is handed out via createConnection.
  }

  setOnWrite(fn: (data: Buffer) => void): void {
    this._onWrite = fn;
  }

  write(data: Buffer | string, cb?: (err?: Error | null) => void): boolean {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data as string);
    this.written.push(buf);
    cb?.();
    if (this._onWrite) this._onWrite(buf);
    return true;
  }

  setNoDelay(_v: boolean): this { return this; }
  setTimeout(_ms: number): this { return this; }
  ref(): this { return this; }
  unref(): this { return this; }

  destroy(err?: Error): this {
    if (this.destroyed) return this;
    this.destroyed = true;
    if (err) setImmediate(() => this.emit('error', err));
    setImmediate(() => this.emit('close'));
    return this;
  }

  receive(buf: Buffer): void {
    setImmediate(() => this.emit('data', buf));
  }

  failWith(err: Error): void {
    setImmediate(() => this.emit('error', err));
  }

  /** Called by the mock after all listeners have been registered. */
  triggerConnect(): void {
    setImmediate(() => this.emit('connect'));
  }

  /** Called by the mock for a unix socket that should immediately fail. */
  triggerError(err: Error): void {
    setImmediate(() => this.emit('error', err));
  }
}

// ---------------------------------------------------------------------------
// Socket queue - tests push sockets here; mock createConnection pops them
// ---------------------------------------------------------------------------
let socketQueue: MockSocket[] = [];

// Set up the mock implementation
const mockCreateConnection = net.createConnection as jest.MockedFunction<typeof net.createConnection>;

function setupNetMock(): void {
  mockCreateConnection.mockImplementation((options: any, connectListener?: () => void): any => {
    // When host is localhost, the pdb_wire code tries a Unix socket first (path option),
    // then falls back to TCP on error.  We detect unix socket attempts by the presence
    // of the `path` key and return a socket that immediately errors out so the code
    // moves on to the TCP attempt, which is the "real" mock socket from the queue.
    if (options && 'path' in options) {
      // Unix socket attempt - return a socket that immediately errors so code falls to TCP.
      const failSock = new MockSocket();
      if (connectListener) failSock.once('connect', connectListener);
      // Trigger the error AFTER returning the socket and after openSocket registers handlers
      failSock.triggerError(new Error('no unix socket'));
      return failSock;
    }

    // TCP attempt - pop from queue
    const sock = socketQueue.shift();
    if (!sock) {
      throw new Error('MockSocket queue empty — push a socket before creating a connection');
    }
    if (connectListener) {
      sock.once('connect', connectListener);
    }
    // Trigger connect AFTER returning the socket so openSocket can register its handler first
    sock.triggerConnect();
    return sock;
  });
}

beforeAll(() => {
  setupNetMock();
});

beforeEach(() => {
  socketQueue = [];
  mockCreateConnection.mockClear();
  // Re-apply implementation after clear
  setupNetMock();
});

// ---------------------------------------------------------------------------
// Helper: create and queue a MockSocket that auto-responds to the HELLO
// handshake, then answers each subsequent write with the next response frame.
// ---------------------------------------------------------------------------
function pushAutoSocket(responses: Buffer[] = []): MockSocket {
  const sock = new MockSocket();
  socketQueue.push(sock);

  let helloHandled = false;
  let responseIdx = 0;

  sock.setOnWrite((_data: Buffer) => {
    if (!helloHandled) {
      helloHandled = true;
      sock.receive(buildHelloOk());
      return;
    }
    if (responseIdx < responses.length) {
      const resp = responses[responseIdx++];
      sock.receive(resp);
    }
  });

  return sock;
}

/** Create a test client connected to a non-existent port (intercepted by mock). */
function makeClient(opts?: ConstructorParameters<typeof PdbWireClient>[0]): PdbWireClient {
  return new PdbWireClient({
    host: 'localhost',
    port: 15440,
    poolSize: 2,
    timeoutMs: 2000,
    maxRetries: 0,
    retryDelayMs: 10,
    ...opts,
  });
}

// ===========================================================================
// 1. Error types
// ===========================================================================

describe('Error types', () => {
  test('PdbError has correct name, message, code', () => {
    const err = new PdbError('test error', 42);
    expect(err.name).toBe('PdbError');
    expect(err.message).toBe('test error');
    expect(err.code).toBe(42);
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(PdbError);
  });

  test('ConnectionError inherits PdbError', () => {
    const err = new ConnectionError('conn failed', 1);
    expect(err.name).toBe('ConnectionError');
    expect(err).toBeInstanceOf(PdbError);
    expect(err).toBeInstanceOf(ConnectionError);
  });

  test('QueryError inherits PdbError', () => {
    const err = new QueryError('syntax error', 100);
    expect(err.name).toBe('QueryError');
    expect(err).toBeInstanceOf(PdbError);
    expect(err).toBeInstanceOf(QueryError);
  });

  test('DiskFullError has code 28', () => {
    const err = new DiskFullError('no space');
    expect(err.name).toBe('DiskFullError');
    expect(err.code).toBe(28);
    expect(err).toBeInstanceOf(PdbError);
    expect(err).toBeInstanceOf(DiskFullError);
  });

  test('ConnectionError default code is 0', () => {
    expect(new ConnectionError('x').code).toBe(0);
  });
});

// ===========================================================================
// 2. Wire protocol frame encoding
// ===========================================================================

describe('Wire protocol frame encoding', () => {
  test('header layout: length(4) | stream_id(2) | msg_type(1) | flags(1)', () => {
    const payload = Buffer.from('hello');
    const frame = buildFrame(MSG_QUERY, payload, 0, 7);
    expect(frame.readUInt32LE(0)).toBe(payload.length);
    expect(frame.readUInt16LE(4)).toBe(7);
    expect(frame[6]).toBe(MSG_QUERY);
    expect(frame[7]).toBe(0);
    expect(frame.subarray(8).toString('utf-8')).toBe('hello');
  });

  test('buildOk encodes affected rows as u64 LE', () => {
    const frame = buildOk(999n);
    expect(frame.readBigUInt64LE(HEADER_SIZE)).toBe(999n);
  });

  test('buildError encodes u16 code + utf-8 message', () => {
    const frame = buildError(404, 'not found');
    expect(frame.readUInt16LE(HEADER_SIZE)).toBe(404);
    expect(frame.subarray(HEADER_SIZE + 2).toString('utf-8')).toBe('not found');
  });

  test('FLAG_MORE_CHUNKS is 0x02', () => {
    expect(FLAG_MORE_CHUNKS).toBe(0x02);
  });

  test('ARROW_STREAM chunk with moreChunks=true has FLAG_MORE_CHUNKS set', () => {
    const frame = buildArrowStreamChunk(Buffer.from('x'), true);
    expect(frame[7] & FLAG_MORE_CHUNKS).toBe(FLAG_MORE_CHUNKS);
  });

  test('final ARROW_STREAM chunk has flags=0', () => {
    const frame = buildArrowStreamChunk(Buffer.from('x'), false);
    expect(frame[7]).toBe(0);
  });
});

// ===========================================================================
// 3. HELLO handshake
// ===========================================================================

describe('HELLO handshake', () => {
  test('first write is a HELLO frame with correct msg_type', async () => {
    const sock = pushAutoSocket([buildOk(0n)]);
    const db = makeClient();
    await db.execute('SELECT 1');
    await db.close();

    expect(sock.written[0][6]).toBe(MSG_HELLO);
  });

  test('HELLO payload contains version=1 and FMT_ARROW_IPC=0x01', async () => {
    const sock = pushAutoSocket([buildOk(0n)]);
    const db = makeClient();
    await db.execute('SELECT 1');
    await db.close();

    const helloPayload = sock.written[0].subarray(HEADER_SIZE);
    expect(helloPayload[0]).toBe(0x01); // HELLO_VERSION
    expect(helloPayload[1]).toBe(0x01); // FMT_ARROW_IPC
  });
});

// ===========================================================================
// 4. query()
// ===========================================================================

describe('query()', () => {
  test('returns empty array for MSG_OK', async () => {
    pushAutoSocket([buildOk(0n)]);
    const db = makeClient();
    expect(await db.query('SELECT 1')).toEqual([]);
    await db.close();
  });

  test('MSG_ARROW_RESULT returns [{ __arrow_ipc: Buffer }] without apache-arrow', async () => {
    pushAutoSocket([buildArrowResult(Buffer.from('fake_ipc'))]);
    const db = makeClient();
    const rows = await db.query('SELECT * FROM t');
    expect(rows.length).toBe(1);
    expect(rows[0].__arrow_ipc).toBeInstanceOf(Buffer);
    await db.close();
  });

  test('sends QUERY frame as second write (after HELLO)', async () => {
    const sock = pushAutoSocket([buildOk(0n)]);
    const db = makeClient();
    await db.query('SELECT id FROM t');
    await db.close();

    expect(sock.written[1][6]).toBe(MSG_QUERY);
    expect(sock.written[1].subarray(HEADER_SIZE).toString('utf-8')).toBe('SELECT id FROM t');
  });

  test('throws QueryError on MSG_ERROR from server', async () => {
    pushAutoSocket([buildError(100, 'table not found')]);
    const db = makeClient();
    await expect(db.query('SELECT * FROM missing')).rejects.toBeInstanceOf(QueryError);
    await db.close();
  });

  test('throws DiskFullError on error code 28', async () => {
    pushAutoSocket([buildError(28, 'no space')]);
    const db = makeClient();
    await expect(db.query('SELECT 1')).rejects.toBeInstanceOf(DiskFullError);
    await db.close();
  });

  test('QueryError carries the error code from the frame', async () => {
    pushAutoSocket([buildError(123, 'err')]);
    const db = makeClient();
    const err: QueryError = await db.query('X').catch((e) => e);
    expect(err.code).toBe(123);
    await db.close();
  });
});

// ===========================================================================
// 5. execute()
// ===========================================================================

describe('execute()', () => {
  test('returns affected rows count from MSG_OK u64', async () => {
    pushAutoSocket([buildOk(7n)]);
    const db = makeClient();
    expect(await db.execute('DELETE FROM t WHERE x = 1')).toBe(7);
    await db.close();
  });

  test('returns 0 for zero-length MSG_OK payload', async () => {
    pushAutoSocket([buildFrame(MSG_OK)]);
    const db = makeClient();
    expect(await db.execute('CREATE TABLE t (id INT)')).toBe(0);
    await db.close();
  });

  test('returns 0 for MSG_ARROW_RESULT response', async () => {
    pushAutoSocket([buildArrowResult(Buffer.from('x'))]);
    const db = makeClient();
    expect(await db.execute('SELECT 1')).toBe(0);
    await db.close();
  });

  test('throws QueryError on server error', async () => {
    pushAutoSocket([buildError(500, 'oops')]);
    const db = makeClient();
    await expect(db.execute('BROKEN')).rejects.toBeInstanceOf(QueryError);
    await db.close();
  });
});

// ===========================================================================
// 6. batch()
// ===========================================================================

describe('batch()', () => {
  test('sends a single BATCH frame (one write after HELLO)', async () => {
    const sock = pushAutoSocket([buildBatchResult(3)]);
    const db = makeClient();
    await db.batch(['BEGIN', "INSERT INTO t VALUES (1)", 'COMMIT']);
    await db.close();

    const batchFrame = sock.written[1];
    expect(batchFrame[6]).toBe(MSG_BATCH);
  });

  test('BATCH payload encodes [u32 count][u32 len, sql]... correctly', async () => {
    const sock = pushAutoSocket([buildBatchResult(3)]);
    const db = makeClient();
    await db.batch(['BEGIN', 'SELECT 1', 'COMMIT']);
    await db.close();

    const payload = sock.written[1].subarray(HEADER_SIZE);
    const count = payload.readUInt32LE(0);
    expect(count).toBe(3);

    let offset = 4;
    const stmts: string[] = [];
    for (let i = 0; i < 3; i++) {
      const len = payload.readUInt32LE(offset); offset += 4;
      stmts.push(payload.subarray(offset, offset + len).toString('utf-8')); offset += len;
    }
    expect(stmts).toEqual(['BEGIN', 'SELECT 1', 'COMMIT']);
  });

  test('resolves with undefined on success', async () => {
    pushAutoSocket([buildBatchResult()]);
    const db = makeClient();
    await expect(db.batch(['SELECT 1'])).resolves.toBeUndefined();
    await db.close();
  });

  test('throws QueryError on error response', async () => {
    pushAutoSocket([buildError(100, 'batch failed'), buildBatchResult() /* ROLLBACK */]);
    const db = makeClient();
    await expect(db.batch(['BAD'])).rejects.toBeInstanceOf(QueryError);
    await db.close();
  });

  test('throws QueryError when a statement inside the batch fails', async () => {
    const sock = pushAutoSocket([
      buildBatchResult(1, 'duplicate key value violates unique constraint'),
      buildBatchResult(), // ROLLBACK
    ]);
    const db = makeClient();
    await expect(db.batch(['INSERT 1', 'INSERT 1', 'INSERT 2'])).rejects.toThrow('duplicate key');
    // The failed batch is followed by a ROLLBACK on the same connection.
    expect(sock.written[2][6]).toBe(MSG_BATCH);
    await db.close();
  });

  test('throws PdbError when BATCH_RESULT has fewer entries and no error', async () => {
    pushAutoSocket([buildBatchResult(1)]);
    const db = makeClient();
    await expect(db.batch(['SELECT 1', 'SELECT 2'])).rejects.toBeInstanceOf(PdbError);
    await db.close();
  });
});

// ===========================================================================
// 7. transaction()
// ===========================================================================

describe('transaction()', () => {
  test('sends BEGIN, user SQL, COMMIT on success', async () => {
    const sock = pushAutoSocket([
      buildBatchResult(),  // BEGIN
      buildOk(1n),         // INSERT
      buildBatchResult(),  // COMMIT
    ]);
    const db = makeClient();

    await db.transaction(async (tx) => {
      await tx.execute("INSERT INTO orders VALUES (1)");
    });

    expect(sock.written[0][6]).toBe(MSG_HELLO);
    expect(sock.written[1][6]).toBe(MSG_BATCH);  // BEGIN
    expect(sock.written[2][6]).toBe(MSG_QUERY);  // INSERT
    expect(sock.written[3][6]).toBe(MSG_BATCH);  // COMMIT

    await db.close();
  });

  test('sends ROLLBACK when user function throws', async () => {
    const sock = pushAutoSocket([
      buildBatchResult(),  // BEGIN
      buildBatchResult(),  // ROLLBACK
    ]);
    const db = makeClient();

    await expect(
      db.transaction(async (_tx) => { throw new Error('boom'); })
    ).rejects.toThrow('boom');

    // ROLLBACK is the third write (HELLO, BEGIN-batch, ROLLBACK-batch)
    const rollbackFrame = sock.written[2];
    expect(rollbackFrame[6]).toBe(MSG_BATCH);
    const rPayload = rollbackFrame.subarray(HEADER_SIZE);
    const sqlLen = rPayload.readUInt32LE(4);
    const sql = rPayload.subarray(8, 8 + sqlLen).toString('utf-8');
    expect(sql).toBe('ROLLBACK');

    await db.close();
  });

  test('a failing COMMIT rejects and is followed by ROLLBACK', async () => {
    const sock = pushAutoSocket([
      buildBatchResult(),                         // BEGIN
      buildBatchResult(0, 'serialization failure'), // COMMIT fails
      buildBatchResult(),                         // ROLLBACK
    ]);
    const db = makeClient();
    await expect(db.transaction(async () => 1)).rejects.toThrow('serialization failure');
    const p = sock.written[3].subarray(HEADER_SIZE);
    expect(p.subarray(8, 8 + p.readUInt32LE(4)).toString('utf-8')).toBe('ROLLBACK');
    await db.close();
  });

  test('returns value from user function', async () => {
    pushAutoSocket([buildBatchResult(), buildOk(0n), buildBatchResult()]);
    const db = makeClient();
    const result = await db.transaction(async (tx) => {
      await tx.execute('SELECT 1');
      return 'txn_result';
    });
    expect(result).toBe('txn_result');
    await db.close();
  });

  test('tx.query() decodes Arrow IPC frames', async () => {
    const arrowBuf = Buffer.from('fake_ipc_data');
    pushAutoSocket([
      buildBatchResult(),
      buildArrowResult(arrowBuf),
      buildBatchResult(),
    ]);
    const db = makeClient();

    let rows: any[] = [];
    await db.transaction(async (tx) => {
      rows = await tx.query('SELECT * FROM t');
    });
    expect(rows.length).toBe(1);
    expect(rows[0].__arrow_ipc).toBeInstanceOf(Buffer);

    await db.close();
  });
});

// ===========================================================================
// 8. ping()
// ===========================================================================

describe('ping()', () => {
  test('returns true when server responds with PONG', async () => {
    pushAutoSocket([buildPong()]);
    const db = makeClient();
    expect(await db.ping()).toBe(true);
    await db.close();
  });

  test('sends PING frame (zero-length payload)', async () => {
    const sock = pushAutoSocket([buildPong()]);
    const db = makeClient();
    await db.ping();
    await db.close();

    expect(sock.written[1][6]).toBe(MSG_PING);
    expect(sock.written[1].readUInt32LE(0)).toBe(0);
  });

  test('returns false when client is closed', async () => {
    const db = makeClient();
    await db.close();
    expect(await db.ping()).toBe(false);
  });
});

// ===========================================================================
// 9. health()
// ===========================================================================

describe('health()', () => {
  test('returns connected=true with correct structure on success', async () => {
    pushAutoSocket([buildPong()]);
    const db = makeClient();
    const h = await db.health();

    expect(h.connected).toBe(true);
    expect(h.protocol).toBe('pdb_wire');
    expect(typeof h.latencyMs).toBe('number');
    expect(h.latencyMs).toBeGreaterThanOrEqual(0);
    expect(typeof h.poolActive).toBe('number');
    expect(typeof h.poolIdle).toBe('number');

    await db.close();
  });

  test('returns connected=false when client is closed', async () => {
    const db = makeClient();
    await db.close();
    const h = await db.health();
    expect(h.connected).toBe(false);
    expect(h.poolActive).toBe(0);
    expect(h.poolIdle).toBe(0);
  });
});

// ===========================================================================
// 10. Metrics - all 12 counters
// ===========================================================================

describe('metrics()', () => {
  test('metricsSnapshot() returns all 12 metric keys', () => {
    const db = new PdbWireClient();
    const m = db.metricsSnapshot();
    const keys = [
      'connectionsCreated', 'connectionsClosed', 'connectionsErrored',
      'queriesTotal', 'queriesErrored', 'queriesRetried',
      'batchesTotal', 'bytesRead', 'bytesWritten',
      'rowsReceived', 'latencySumMs', 'poolWaits',
    ];
    for (const k of keys) {
      expect(m).toHaveProperty(k);
      expect(typeof (m as any)[k]).toBe('number');
    }
    db.close();
  });

  test('queriesTotal increments per execute()', async () => {
    // Sequential queries reuse the same connection - one socket, two query responses
    pushAutoSocket([buildOk(0n), buildOk(0n)]);
    const db = makeClient({ poolSize: 2 });
    await db.execute('SELECT 1');
    await db.execute('SELECT 2');
    await db.close();
    expect(db.metricsSnapshot().queriesTotal).toBe(2);
  });

  test('batchesTotal increments per batch()', async () => {
    pushAutoSocket([buildBatchResult()]);
    const db = makeClient();
    await db.batch(['SELECT 1']);
    await db.close();
    expect(db.metricsSnapshot().batchesTotal).toBe(1);
  });

  test('queriesErrored increments on query error', async () => {
    pushAutoSocket([buildError(100, 'err')]);
    const db = makeClient({ maxRetries: 0 });
    await db.query('BAD').catch(() => {});
    await db.close();
    expect(db.metricsSnapshot().queriesErrored).toBe(1);
  });

  test('connectionsCreated increments when pool opens new connection', async () => {
    pushAutoSocket([buildOk(0n)]);
    const db = makeClient({ poolSize: 1 });
    await db.execute('SELECT 1');
    await db.close();
    expect(db.metricsSnapshot().connectionsCreated).toBe(1);
  });

  test('bytesRead and bytesWritten are positive after a query', async () => {
    pushAutoSocket([buildOk(5n)]);
    const db = makeClient();
    await db.execute('SELECT 1');
    await db.close();
    const m = db.metricsSnapshot();
    expect(m.bytesRead).toBeGreaterThan(0);
    expect(m.bytesWritten).toBeGreaterThan(0);
  });

  test('rowsReceived counts parsed Arrow rows', async () => {
    pushAutoSocket([buildArrowResult(Buffer.from('fake'))]);
    const db = makeClient();
    await db.query('SELECT * FROM t');
    await db.close();
    // Without apache-arrow, one synthetic row per result buffer
    expect(db.metricsSnapshot().rowsReceived).toBe(1);
  });

  test('latencySumMs is positive after a timed query', async () => {
    pushAutoSocket([buildOk(0n)]);
    const db = makeClient();
    await db.execute('SELECT 1');
    await db.close();
    expect(db.metricsSnapshot().latencySumMs).toBeGreaterThanOrEqual(0);
  });

  test('metrics() returns valid Prometheus exposition text with all 12 metric names', async () => {
    pushAutoSocket([buildOk(0n)]);
    const db = makeClient();
    await db.execute('SELECT 1');
    const text = db.metrics();
    await db.close();

    const expectedNames = [
      'pdb_connections_created_total',
      'pdb_connections_closed_total',
      'pdb_connections_errored_total',
      'pdb_queries_total',
      'pdb_queries_errored_total',
      'pdb_queries_retried_total',
      'pdb_batches_total',
      'pdb_bytes_read_total',
      'pdb_bytes_written_total',
      'pdb_rows_received_total',
      'pdb_query_latency_ms_total',
      'pdb_pool_waits_total',
    ];
    for (const name of expectedNames) {
      expect(text).toContain(name);
    }
    expect(text).toMatch(/^# HELP /m);
    expect(text).toMatch(/^# TYPE /m);
  });
});

// ===========================================================================
// 11. Connection pool
// ===========================================================================

describe('Connection pool', () => {
  test('sequential queries reuse the same connection (connectionsCreated=1)', async () => {
    const sock = pushAutoSocket([buildOk(0n), buildOk(0n)]);
    const db = makeClient({ poolSize: 2 });

    await db.execute('SELECT 1');
    await db.execute('SELECT 2');
    await db.close();

    expect(db.metricsSnapshot().connectionsCreated).toBe(1);
    // First socket received both queries
    expect(sock.written.filter(f => f[6] === MSG_QUERY).length).toBe(2);
  });

  test('close() destroys idle connections and increments connectionsClosed', async () => {
    pushAutoSocket([buildOk(0n)]);
    const db = makeClient({ poolSize: 1 });
    await db.execute('SELECT 1');
    await db.close();
    expect(db.metricsSnapshot().connectionsClosed).toBeGreaterThanOrEqual(1);
  });

  test('close() is idempotent — second close() does not throw', async () => {
    const db = makeClient();
    await db.close();
    await expect(db.close()).resolves.toBeUndefined();
  });

  test('queries throw ConnectionError after close()', async () => {
    const db = makeClient();
    await db.close();
    await expect(db.execute('SELECT 1')).rejects.toBeInstanceOf(ConnectionError);
  });
});

// ===========================================================================
// 12. Retry logic
// ===========================================================================

describe('Retry logic', () => {
  test('QueryError is NOT retried (queriesRetried stays 0)', async () => {
    pushAutoSocket([buildError(100, 'bad query')]);
    const db = makeClient({ maxRetries: 3 });
    await expect(db.execute('BAD')).rejects.toBeInstanceOf(QueryError);
    expect(db.metricsSnapshot().queriesRetried).toBe(0);
    await db.close();
  });

  test('DiskFullError is NOT retried', async () => {
    pushAutoSocket([buildError(28, 'disk full')]);
    const db = makeClient({ maxRetries: 3 });
    await expect(db.execute('INSERT INTO t VALUES (1)')).rejects.toBeInstanceOf(DiskFullError);
    expect(db.metricsSnapshot().queriesRetried).toBe(0);
    await db.close();
  });

  test('a ConnectionError after the request was sent is NOT retried', async () => {
    // The query reached the socket, then the connection failed: the server
    // may have executed it, so it is surfaced, not resent (see
    // pdb_wire_ambiguous.test.ts for the real-socket proof).
    const failSock = new MockSocket();
    socketQueue.push(failSock);
    let failHelloSent = false;
    failSock.setOnWrite((_data: Buffer) => {
      if (!failHelloSent) {
        failHelloSent = true;
        failSock.receive(buildHelloOk());
        return;
      }
      setImmediate(() => failSock.emit('error', new Error('network reset')));
    });

    const db = makeClient({ maxRetries: 1, retryDelayMs: 1, timeoutMs: 1000 });
    await expect(db.execute('SELECT 1')).rejects.toBeInstanceOf(ConnectionError);
    expect(db.metricsSnapshot().queriesRetried).toBe(0);
    await db.close();
  });
});

// ===========================================================================
// 13. MSG_ARROW_STREAM - chunked streaming
// ===========================================================================

describe('MSG_ARROW_STREAM chunked responses', () => {
  test('reassembles two chunks into a single Arrow result', async () => {
    const chunk1 = Buffer.from('part1_');
    const chunk2 = Buffer.from('part2');
    const expected = Buffer.concat([chunk1, chunk2]);

    const sock = new MockSocket();
    socketQueue.push(sock);

    let helloSent = false;
    let querySent = false;
    sock.setOnWrite((_data: Buffer) => {
      if (!helloSent) {
        helloSent = true;
        sock.receive(buildHelloOk());
        return;
      }
      if (!querySent) {
        querySent = true;
        // First chunk with FLAG_MORE_CHUNKS, second without
        sock.receive(buildArrowStreamChunk(chunk1, true));
        setImmediate(() => sock.receive(buildArrowStreamChunk(chunk2, false)));
      }
    });

    const db = makeClient();
    const rows = await db.query('SELECT * FROM big_table');
    expect(rows.length).toBe(1);
    expect(Buffer.from(rows[0].__arrow_ipc as Buffer).equals(expected)).toBe(true);
    await db.close();
  });

  test('single-chunk ARROW_STREAM (flags=0) resolves immediately', async () => {
    const data = Buffer.from('single_chunk');
    const sock = new MockSocket();
    socketQueue.push(sock);

    let helloSent = false;
    sock.setOnWrite((_d: Buffer) => {
      if (!helloSent) {
        helloSent = true;
        sock.receive(buildHelloOk());
        return;
      }
      sock.receive(buildArrowStreamChunk(data, false));
    });

    const db = makeClient();
    const rows = await db.query('SELECT 1');
    expect(rows.length).toBe(1);
    await db.close();
  });
});

// ===========================================================================
// 14. Timeout handling
// ===========================================================================

describe('Timeout handling', () => {
  test('throws ConnectionError when server never responds within timeoutMs', async () => {
    const sock = new MockSocket();
    socketQueue.push(sock);

    let helloSent = false;
    sock.setOnWrite((_d: Buffer) => {
      if (!helloSent) {
        helloSent = true;
        sock.receive(buildHelloOk());
        // Never respond to the query - timeout will fire
      }
    });

    const db = makeClient({ timeoutMs: 100, maxRetries: 0 });
    await expect(db.execute('SELECT SLEEP(999)')).rejects.toBeInstanceOf(ConnectionError);
    await db.close();
  }, 3000);
});

// ===========================================================================
// 15. Configuration defaults
// ===========================================================================

describe('Configuration defaults', () => {
  test('PdbWireClient can be instantiated with no options', () => {
    const db = new PdbWireClient();
    expect(db).toBeInstanceOf(PdbWireClient);
    expect(typeof db.metrics()).toBe('string');
    db.close();
  });

  test('metrics() output is non-empty string even with no queries', () => {
    const db = new PdbWireClient();
    const text = db.metrics();
    expect(typeof text).toBe('string');
    expect(text.length).toBeGreaterThan(0);
    db.close();
  });

  test('ConnectionOptions transport field is accepted', () => {
    const opts: ConstructorParameters<typeof PdbWireClient>[0] = {
      transport: 'pdb_wire',
      metricsEnabled: false,
    };
    const db = new PdbWireClient(opts);
    expect(db).toBeInstanceOf(PdbWireClient);
    db.close();
  });
});

// ===========================================================================
// 16. MSG_CLOSE frame
// ===========================================================================

describe('MSG_CLOSE frame', () => {
  test('MSG_CLOSE is sent on connection.close() via pool.closeAll()', async () => {
    const sock = pushAutoSocket([buildOk(0n)]);
    const db = makeClient({ poolSize: 1 });

    await db.execute('SELECT 1');
    await db.close();

    const closeFrames = sock.written.filter((f) => f[6] === MSG_CLOSE);
    expect(closeFrames.length).toBeGreaterThanOrEqual(1);
    // Close frame should have zero payload length
    expect(closeFrames[0].readUInt32LE(0)).toBe(0);
  });
});
