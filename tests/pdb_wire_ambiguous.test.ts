/**
 * Ambiguous transport failures are surfaced, never retried, and the
 * connection is discarded.
 *
 * Once any byte of a request may have reached the server, a socket error,
 * EOF or client-side timeout is AMBIGUOUS: the server may have executed and
 * committed it. Resending applies the writes twice (a SELECT is not
 * necessarily read-only either: `SELECT KV_SET(...)`), and pooling the
 * connection hands the next caller a socket that may still deliver the late
 * reply, or that is still marked busy with the timed-out request.
 *
 * A real TCP fake server (no ParticleDB) counts the requests it read and the
 * connections it accepted. Only a failure before any byte of the request was
 * written (connect refused, HELLO dropped on a fresh connection) is retried.
 *
 *   node --import tsx --test tests/pdb_wire_ambiguous.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as net from 'net';

import { PdbWireClient, ConnectionError } from '../src/pdb_wire';

type Act = 'close' | 'hang' | 'ok';

const MSG_HELLO = 0x00;
const MSG_QUERY = 0x01;
const MSG_BATCH = 0x02;
const MSG_HELLO_OK = 0x80;
const MSG_OK = 0x82;
const MSG_BATCH_RESULT = 0x85;

function frame(type: number, sid: number, body: Buffer): Buffer {
  const h = Buffer.alloc(8);
  h.writeUInt32LE(body.length, 0);
  h.writeUInt16LE(sid, 4);
  h[6] = type;
  return Buffer.concat([h, body]);
}

class FakeServer {
  accepts = 0;
  requests = 0;
  readonly sockets: net.Socket[] = [];
  private server: net.Server;

  constructor(private plan: Act[], private dropHello = 0) {
    this.server = net.createServer((s) => this.serve(s));
  }

  listen(port = 0): Promise<number> {
    return new Promise((resolve) => {
      this.server.listen(port, '127.0.0.1', () => {
        resolve((this.server.address() as net.AddressInfo).port);
      });
    });
  }

  private serve(s: net.Socket): void {
    this.accepts++;
    this.sockets.push(s);
    s.on('error', () => {});
    let buf = Buffer.alloc(0);
    s.on('data', (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      while (buf.length >= 8) {
        const plen = buf.readUInt32LE(0);
        if (buf.length < 8 + plen) return;
        const sid = buf.readUInt16LE(4);
        const type = buf[6];
        const payload = buf.subarray(8, 8 + plen);
        buf = buf.subarray(8 + plen);
        if (type === MSG_HELLO) {
          if (this.dropHello > 0) {
            this.dropHello--;
            s.destroy();
            return;
          }
          s.write(frame(MSG_HELLO_OK, sid, Buffer.from([1, 1, 0])));
          continue;
        }
        if (type !== MSG_QUERY && type !== MSG_BATCH) continue;
        this.requests++;
        const act = this.plan.shift() ?? 'ok';
        if (act === 'close') {
          s.destroy();
          return;
        }
        if (act === 'hang') continue;
        if (type === MSG_QUERY) {
          const b = Buffer.alloc(8);
          b.writeBigUInt64LE(1n, 0);
          s.write(frame(MSG_OK, sid, b));
        } else {
          const n = payload.readUInt32LE(0);
          const b = Buffer.alloc(4 + 9 * n);
          b.writeUInt32LE(n, 0);
          for (let i = 0; i < n; i++) {
            b[4 + 9 * i] = MSG_OK;
            b.writeBigUInt64LE(1n, 5 + 9 * i);
          }
          s.write(frame(MSG_BATCH_RESULT, sid, b));
        }
      }
    });
  }

  close(): Promise<void> {
    for (const s of this.sockets) s.destroy();
    return new Promise((resolve) => this.server.close(() => resolve()));
  }
}

async function rejection(p: Promise<unknown>): Promise<unknown> {
  try {
    await p;
  } catch (e) {
    return e;
  }
  return undefined;
}

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  while (cleanups.length) await cleanups.pop()!();
});

async function setup(plan: Act[], opts: { dropHello?: number; timeoutMs?: number } = {}) {
  const srv = new FakeServer(plan, opts.dropHello ?? 0);
  const port = await srv.listen();
  const db = new PdbWireClient({
    host: '127.0.0.1',
    port,
    poolSize: 2,
    timeoutMs: opts.timeoutMs ?? 5000,
    maxRetries: 3,
    retryDelayMs: 20,
  });
  cleanups.push(() => srv.close(), () => db.close());
  return { srv, db };
}

describe('native wire: ambiguous failures are not retried', () => {
  it('batch dropped after send: raised, sent once, connection discarded', async () => {
    const { srv, db } = await setup(['close']);
    const err = await rejection(db.batch(['INSERT INTO t VALUES (1)', 'INSERT INTO t VALUES (2)']));
    assert.equal(srv.requests, 1, 'the batch was resent');
    assert.ok(err instanceof ConnectionError, `expected ConnectionError, got ${String(err)}`);
    await db.batch(['INSERT INTO t VALUES (3)']);
    assert.equal(srv.accepts, 2, 'the dead connection was reused');
  });

  it('statement dropped after send: raised, sent once', async () => {
    const { srv, db } = await setup(['close']);
    const err = await rejection(db.execute("SELECT KV_SET('k', 'v')"));
    assert.equal(srv.requests, 1, 'the statement was resent');
    assert.ok(err instanceof ConnectionError, `expected ConnectionError, got ${String(err)}`);
    assert.equal(await db.execute('SELECT 1'), 1);
    assert.equal(srv.accepts, 2);
  });

  it('timeout: raised, sent once, socket with the late reply discarded', async () => {
    const { srv, db } = await setup(['hang'], { timeoutMs: 300 });
    const err = await rejection(db.query('UPDATE t SET x = x + 1'));
    assert.equal(srv.requests, 1, 'the timed-out request was resent');
    assert.ok(err instanceof ConnectionError, `expected ConnectionError, got ${String(err)}`);
    assert.equal(await db.execute('SELECT 1'), 1);
    assert.equal(srv.accepts, 2, 'the timed-out connection was returned to the pool');
  });

  it('transaction COMMIT timing out: raised once, connection discarded', async () => {
    // BEGIN ok, INSERT ok, COMMIT hangs.
    const { srv, db } = await setup(['ok', 'ok', 'hang'], { timeoutMs: 300 });
    const err = await rejection(
      db.transaction(async (tx) => {
        await tx.execute('INSERT INTO t VALUES (1)');
      }),
    );
    assert.ok(err instanceof ConnectionError, `expected ConnectionError, got ${String(err)}`);
    assert.equal(srv.requests, 3, 'a statement of the transaction was resent');
    assert.equal(await db.execute('SELECT 1'), 1);
    assert.equal(srv.accepts, 2, 'the connection with the in-flight COMMIT was reused');
  });

  it('a dropped HELLO on a fresh connection is retried', async () => {
    const { srv, db } = await setup([], { dropHello: 1 });
    await db.batch(['INSERT INTO t VALUES (1)']);
    assert.equal(srv.accepts, 2);
    assert.equal(srv.requests, 1);
  });

  it('a refused connect is retried', async () => {
    const probe = new FakeServer([]);
    const port = await probe.listen();
    await probe.close(); // nothing listens on `port` now: the connect is refused
    const srv = new FakeServer([]);
    const db = new PdbWireClient({ host: '127.0.0.1', port, poolSize: 1, maxRetries: 4, retryDelayMs: 100 });
    cleanups.push(() => srv.close(), () => db.close());
    setTimeout(() => void srv.listen(port), 150); // inside the 100/200/400 ms backoff
    assert.equal(await db.execute('SELECT 1'), 1);
    assert.equal(srv.requests, 1);
  });
});
