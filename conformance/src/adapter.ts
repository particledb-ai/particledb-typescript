/**
 * The TypeScript SDK's conformance adapter (package
 * `@particledb/sdk`), run by `../run_conformance.sh`.
 *
 * It drives the client through the driver-layer surface the ORM adapters
 * (`@particledb/drizzle`, `@particledb/prisma`) are built on -- `queryArrays` (positional rows
 * plus the server's column descriptions) and `reserve` (one dedicated
 * connection for an explicit transaction) -- and the documented `ParticleDB`
 * facade for the rest (executeBatch, kv). Every connection is the zero-option
 * default apart from host/port, because the DEFAULTS are under test.
 *
 * Contract: the conformance specification (conformance/SPEC.md), section 5 (env in, one results file out).
 */
import { spawnSync } from 'node:child_process';
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { createConnection, type Socket } from 'node:net';
import { join } from 'node:path';
import {
  BatchExecuteError,
  ParticleDB,
  PdbWireClient,
  type RawQueryResult,
  type ReservedConnection,
  type ResultField,
} from '@particledb/sdk';

const CASE_TIMEOUT_MS = 180_000;
const SDK_VERSION: string = JSON.parse(readFileSync(join(__dirname, '..', '..', 'package.json'), 'utf8')).version;

class Skip extends Error {}
class Fail extends Error {}

function check(ok: boolean, detail: string): void {
  if (!ok) throw new Fail(detail);
}
function show(o: unknown): string {
  const s = o === null || o === undefined ? 'null' : typeof o === 'string' ? `'${o}'` : JSON.stringify(o, (_k, v) => (typeof v === 'bigint' ? String(v) : v));
  return s.length <= 200 ? s : `${s.slice(0, 200)}...(${s.length} chars)`;
}
function eq(want: unknown, got: unknown, what: string): void {
  if (JSON.stringify(want, big) !== JSON.stringify(got, big)) throw new Fail(`${what}: expected ${show(want)}, got ${show(got)}`);
}
const big = (_k: string, v: unknown) => (typeof v === 'bigint' ? `${v}n` : v);

function env(k: string): string {
  const v = process.env[k];
  if (!v) throw new Error(`${k} is not set (run via run_conformance.sh)`);
  return v;
}
const HOST = process.env.PDB_CONF_HOST ?? '127.0.0.1';
const port = () => Number(env('PDB_CONF_PORT'));
const defaultPort = () => Number(env('PDB_CONF_DEFAULT_PORT'));
const respPort = () => Number(env('PDB_CONF_RESP_PORT'));
const mode = () => env('PDB_CONF_MODE');

function control(action: string): void {
  const r = spawnSync(env('PDB_CONF_CONTROL'), [action], { encoding: 'utf8', timeout: 120_000 });
  if (r.status !== 0) throw new Error(`control ${action} failed: ${r.stdout}${r.stderr}`);
}

/** The documented client, on the runner's port. */
function client(poolSize?: number): PdbWireClient {
  return new PdbWireClient({ host: HOST, port: port(), ...(poolSize ? { poolSize } : {}) });
}

// ---------------------------------------------------------------------------
// A minimal RESP2 client (the independent KV reader / writer)
// ---------------------------------------------------------------------------

class Resp {
  private buf = Buffer.alloc(0);
  private waiters: Array<() => void> = [];
  private constructor(private sock: Socket) {
    sock.on('data', (d) => {
      this.buf = Buffer.concat([this.buf, d]);
      const w = this.waiters.splice(0);
      w.forEach((f) => f());
    });
  }
  static open(): Promise<Resp> {
    return new Promise((resolve, reject) => {
      const s = createConnection({ host: HOST, port: respPort() }, () => resolve(new Resp(s)));
      s.once('error', reject);
    });
  }
  private async need(n: number): Promise<void> {
    while (this.buf.length < n) await new Promise<void>((r) => this.waiters.push(r));
  }
  private async line(): Promise<string> {
    for (;;) {
      const i = this.buf.indexOf('\r\n');
      if (i >= 0) {
        const l = this.buf.subarray(0, i).toString('utf8');
        this.buf = this.buf.subarray(i + 2);
        return l;
      }
      await this.need(this.buf.length + 1);
    }
  }
  private async read(): Promise<unknown> {
    const l = await this.line();
    const t = l[0];
    const rest = l.slice(1);
    if (t === '+') return Buffer.from(rest);
    if (t === '-') throw new Error(`RESP error: ${rest}`);
    if (t === ':') return Number(rest);
    if (t === '$') {
      const n = Number(rest);
      if (n < 0) return null;
      await this.need(n + 2);
      const b = Buffer.from(this.buf.subarray(0, n));
      this.buf = this.buf.subarray(n + 2);
      return b;
    }
    if (t === '*') {
      const n = Number(rest);
      if (n < 0) return null;
      const out: unknown[] = [];
      for (let i = 0; i < n; i++) out.push(await this.read());
      return out;
    }
    throw new Error(`RESP: unexpected reply ${l}`);
  }
  async cmd(...args: Array<string | Buffer>): Promise<unknown> {
    const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
    for (const a of args) {
      const v = Buffer.isBuffer(a) ? a : Buffer.from(a, 'utf8');
      parts.push(Buffer.from(`$${v.length}\r\n`), v, Buffer.from('\r\n'));
    }
    this.sock.write(Buffer.concat(parts));
    return this.read();
  }
  async str(...args: string[]): Promise<string | null> {
    const r = await this.cmd(...args);
    return r === null ? null : Buffer.isBuffer(r) ? r.toString('utf8') : String(r);
  }
  async num(...args: string[]): Promise<number> {
    const r = await this.cmd(...args);
    if (typeof r !== 'number') throw new Fail(`RESP ${args[0]} answered ${show(r)}`);
    return r;
  }
  close(): void {
    this.sock.destroy();
  }
}

// ---------------------------------------------------------------------------
// Parameter binding and canonical rendering (CONFORMANCE.md section 4)
// ---------------------------------------------------------------------------

interface Param {
  type: string;
  value: unknown;
}

function unhex(s: string): Buffer {
  if (!s.startsWith('\\x')) throw new Error(`bytea canonical form is \\x<hex>: ${s}`);
  return Buffer.from(s.slice(2), 'hex');
}

/** A case file's canonical text -> the JS value an application binds. */
function scalar(t: string, v: string): unknown {
  switch (t) {
    case 'int2': case 'int4':
      return Number(v);
    case 'int8': {
      const b = BigInt(v);
      return b >= BigInt(Number.MIN_SAFE_INTEGER) && b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(b) : b;
    }
    case 'float4': case 'float8':
      return v === 'NaN' ? NaN : v === 'Infinity' ? Infinity : v === '-Infinity' ? -Infinity : Number(v);
    case 'bool':
      return v === 'true';
    case 'bytea':
      return unhex(v);
    default:
      return v; // numeric, text, json, date, timestamp(tz), uuid: canonical text
  }
}

function bind(params: Param[]): unknown[] {
  return params.map((p) => {
    if (p.value === null || p.value === undefined) return null;
    if (p.type === 'vector') return `[${(p.value as unknown[]).join(',')}]`;
    if (p.type.endsWith('[]')) return (p.value as Array<string | null>).map((e) => (e === null ? null : scalar(p.type.slice(0, -2), e)));
    return scalar(p.type, p.value as string);
  });
}

function canonFloat(d: number): string {
  if (Number.isNaN(d)) return 'NaN';
  if (!Number.isFinite(d)) return d > 0 ? 'Infinity' : '-Infinity';
  return String(d);
}

function parsePgArray(text: string): unknown[] {
  let i = 0;
  const parse = (): unknown[] => {
    if (text[i] !== '{') throw new Fail(`malformed array literal ${show(text)}`);
    i++;
    const out: unknown[] = [];
    if (text[i] === '}') { i++; return out; }
    for (;;) {
      if (text[i] === '{') out.push(parse());
      else if (text[i] === '"') {
        i++;
        let s = '';
        while (i < text.length && text[i] !== '"') { if (text[i] === '\\') i++; s += text[i++]; }
        i++;
        out.push(s);
      } else {
        let s = '';
        while (i < text.length && text[i] !== ',' && text[i] !== '}') s += text[i++];
        out.push(s.trim().toUpperCase() === 'NULL' ? null : s.trim());
      }
      if (text[i] === ',') { i++; continue; }
      if (text[i] === '}') { i++; return out; }
      throw new Fail(`malformed array literal ${show(text)}`);
    }
  };
  return parse();
}

/** A decoded value of `field` in the case files' canonical form for `t`. */
function canon(t: string, v: unknown, field?: ResultField): unknown {
  if (v === null || v === undefined) return null;
  if (t.endsWith('[]')) {
    const et = t.slice(0, -2);
    const items = typeof v === 'string' ? parsePgArray(v) : (v as unknown[]);
    check(Array.isArray(items), `${t} column decoded as ${typeof v}: ${show(v)}`);
    return items.map((e) => canon(et, e, typeof v === 'string' ? undefined : field?.element ?? field));
  }
  switch (t) {
    case 'vector': {
      if (Buffer.isBuffer(v)) {
        const out: string[] = [];
        for (let k = 0; k + 4 <= v.length; k += 4) out.push(canonFloat(v.readFloatLE(k)));
        return out;
      }
      const items = typeof v === 'string' ? JSON.parse(v) : v;
      return (items as number[]).map((x) => canonFloat(Number(x)));
    }
    case 'int2': case 'int4': case 'int8':
      // Array elements arrive as PostgreSQL array text (the declared type is
      // stamped `INT4[]` etc.) and are parsed here, as the ORM adapters do.
      check(typeof v === 'number' || typeof v === 'bigint' || (field === undefined && /^-?\d+$/.test(String(v))), `${t} decoded as ${typeof v}`);
      return String(v);
    case 'float4': case 'float8':
      check(typeof v === 'number', `${t} decoded as ${typeof v}`);
      return canonFloat(v as number);
    case 'numeric':
      check(typeof v === 'string', `numeric decoded as ${typeof v}: ${show(v)}`);
      return v;
    case 'bool':
      check(typeof v === 'boolean', `bool decoded as ${typeof v}`);
      return v ? 'true' : 'false';
    case 'bytea':
      check(Buffer.isBuffer(v), `bytea decoded as ${typeof v}`);
      return `\\x${(v as Buffer).toString('hex')}`;
    case 'date':
      return String(v);
    case 'timestamp':
      check(typeof v === 'string', `timestamp decoded as ${typeof v}`);
      return v;
    case 'timestamptz':
      check(typeof v === 'string', `timestamptz decoded as ${typeof v}`);
      return `${v}+00`;
    case 'uuid':
      return String(v).toLowerCase();
    default:
      check(typeof v === 'string', `${t} decoded as ${typeof v}: ${show(v)}`);
      return v;
  }
}

function cellEquals(t: string, want: unknown, got: unknown): boolean {
  if (want === null || got === null) return want === null && got === null;
  if (t.endsWith('[]') || t === 'vector') {
    if (!Array.isArray(want) || !Array.isArray(got) || want.length !== got.length) return false;
    const et = t === 'vector' ? 'float4' : t.slice(0, -2);
    return want.every((w, k) => cellEquals(et, w, got[k]));
  }
  if (t === 'float4' || t === 'float8') {
    const a = Number(String(want).replace('Infinity', 'Infinity'));
    const b = Number(got);
    if (Number.isNaN(a) || Number.isNaN(b)) return Number.isNaN(a) && Number.isNaN(b);
    return t === 'float4' ? Math.fround(a) === Math.fround(b) : a === b;
  }
  return String(want) === String(got);
}

function gen(g: Record<string, unknown>): string {
  const count = Number(g.count);
  if ('repeat' in g) return String(g.repeat).repeat(count);
  return `\\x${Buffer.from(Array.from({ length: 256 }, (_, k) => k)).toString('hex').repeat(count)}`;
}

// ---------------------------------------------------------------------------
// Sessions: one reserved connection per script `conn`
// ---------------------------------------------------------------------------

const DECODE = { timestamps: 'string' } as const;

class Session {
  private constructor(private readonly db: PdbWireClient, private readonly conn: ReservedConnection) {}
  static async open(): Promise<Session> {
    const db = client(1);
    return new Session(db, await db.reserve());
  }
  run(sql: string, params: Param[]): Promise<RawQueryResult> {
    return this.conn.queryArrays(sql, params.length ? bind(params) : undefined, DECODE);
  }
  async query(sql: string, params: Param[], types: string[]): Promise<unknown[][]> {
    const r = await this.run(sql, params);
    return r.rows.map((row) => row.map((v, k) => canon(types[k], v, r.fields[k])));
  }
  async begin() { await this.conn.queryArrays('BEGIN'); }
  async commit() {
    const r = await this.conn.queryArrays('COMMIT');
    if (r.rolledBack) throw Object.assign(new Error('COMMIT rolled back a failed block'), { sqlstate: '25P02' });
  }
  async rollback() { await this.conn.queryArrays('ROLLBACK'); }
  async isolation(level: string) { await this.conn.queryArrays(`SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ${level}`); }
  async savepoint(name: string) { await this.conn.queryArrays(`SAVEPOINT ${name}`); }
  async rollbackTo(name: string) { await this.conn.queryArrays(`ROLLBACK TO SAVEPOINT ${name}`); }
  async close() {
    await this.conn.release().catch(() => undefined);
    await this.db.close().catch(() => undefined);
  }
}

function sqlstate(e: unknown): string | undefined {
  let c = e as { sqlstate?: string; cause?: unknown } | undefined;
  for (let d = 0; c && d < 5; d++) {
    if (c.sqlstate) return c.sqlstate;
    c = c.cause as typeof c;
  }
  return undefined;
}

interface Step {
  op: string;
  conn?: string;
  sql?: string;
  params?: Array<{ type: string; value?: unknown; gen?: Record<string, unknown> }>;
  types?: string[];
  expect_rows?: unknown[][];
  sqlstate?: string;
  level?: string;
  name?: string;
}

async function runScript(c: { steps: Step[] }): Promise<void> {
  const sessions = new Map<string, Session>();
  const skipNext = new Set<string>();
  try {
    for (let n = 1; n <= c.steps.length; n++) {
      const st = c.steps[n - 1];
      const conn = st.conn ?? 'a';
      let s = sessions.get(conn);
      if (!s) { s = await Session.open(); sessions.set(conn, s); }
      const params: Param[] = (st.params ?? []).map((p) => ({ type: p.type, value: p.gen ? gen(p.gen) : p.value ?? null }));
      const where = `step ${n} (${st.op}${st.sql ? ' ' + (st.sql.length > 60 ? st.sql.slice(0, 57) + '...' : st.sql) : ''})`;
      try {
        switch (st.op) {
          case 'exec': await s.run(st.sql!, params); break;
          case 'query': {
            const got = await s.query(st.sql!, params, st.types!);
            const want = st.expect_rows!;
            if (want.length !== got.length) throw new Fail(`${where}: expected ${want.length} rows, got ${got.length} ${show(got)}`);
            want.forEach((wr, r) => {
              if (wr.length !== got[r].length) throw new Fail(`${where}: row ${r} width ${got[r].length} != ${wr.length}`);
              wr.forEach((w0, col) => {
                const w = w0 !== null && typeof w0 === 'object' && !Array.isArray(w0) ? gen((w0 as { gen: Record<string, unknown> }).gen) : w0;
                if (!cellEquals(st.types![col], w, got[r][col])) {
                  throw new Fail(`${where}: row ${r} col ${col + 1} (${st.types![col]}): expected ${show(w)}, got ${show(got[r][col])}`);
                }
              });
            });
            break;
          }
          case 'error': {
            let failed = false;
            try { await s.run(st.sql!, params); } catch (e) {
              if (e instanceof Fail) throw e;
              eq(st.sqlstate, sqlstate(e), `${where} SQLSTATE (${(e as Error).message})`);
              failed = true;
            }
            if (!failed) throw new Fail(`${where}: expected SQLSTATE ${st.sqlstate}, statement succeeded`);
            break;
          }
          case 'begin': await s.begin(); break;
          case 'commit': await s.commit(); break;
          case 'rollback': await s.rollback(); break;
          case 'isolation': await s.isolation(st.level!); break;
          case 'savepoint': await s.savepoint(st.name!); break;
          case 'rollback_to': await s.rollbackTo(st.name!); break;
          case 'commit_error': {
            if (skipNext.delete(conn)) break;
            let failed = false;
            try { await s.commit(); } catch (e) {
              eq(st.sqlstate, sqlstate(e), `${where} SQLSTATE (${(e as Error).message})`);
              await s.rollback().catch(() => undefined);
              failed = true;
            }
            if (!failed) throw new Fail(`${where}: expected the commit to fail ${st.sqlstate}, it succeeded`);
            break;
          }
          default: throw new Fail(`unknown script op ${st.op}`);
        }
      } catch (e) {
        if (e instanceof Fail || e instanceof Skip) throw e;
        const stt = sqlstate(e);
        const nxt = c.steps.slice(n).find((m) => (m.conn ?? 'a') === conn);
        if (nxt && nxt.op === 'commit_error' && stt && stt === nxt.sqlstate) {
          await s.rollback().catch(() => undefined);
          skipNext.add(conn);
          continue;
        }
        throw new Fail(`${where}: ${(e as Error).name}: ${(e as Error).message}${stt ? ` [SQLSTATE ${stt}]` : ''}`);
      }
    }
  } finally {
    for (const s of sessions.values()) await s.close();
  }
}

// ---------------------------------------------------------------------------
// Custom cases
// ---------------------------------------------------------------------------

async function one(c: PdbWireClient, sql: string, params?: unknown[]): Promise<unknown> {
  const r = await c.queryArrays(sql, params);
  check(r.rows.length > 0, `no row from ${sql}`);
  return r.rows[0][0];
}
const caps = (csv: unknown) => String(csv).split(',').map((x) => x.trim()).filter(Boolean).sort();
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function withClient<T>(fn: (c: PdbWireClient) => Promise<T>, poolSize?: number): Promise<T> {
  const c = client(poolSize);
  try { return await fn(c); } finally { await c.close().catch(() => undefined); }
}

const NO_MUX = 'the TypeScript SDK speaks native protocol 1 (one request per pooled connection): it offers no multiplexed connection to share';

function customs(defs: Record<string, any>): Record<string, () => Promise<void>> {
  const required: string[] = defs['DEF-002'].required_capabilities;
  const cases: Record<string, () => Promise<void>> = {};

  // ── DEFAULTS ──
  cases['DEF-001'] = () => withClient(async (c) => eq('pdb-native-v2', await one(c, 'SHOW pdb.wire_protocol'), 'server-recorded protocol'));
  cases['DEF-002'] = () => withClient(async (c) => {
    const got = caps(await one(c, 'SHOW pdb.wire_capabilities'));
    const missing = required.filter((r) => !got.includes(r));
    check(missing.length === 0, `server recorded ${show(got)}; missing required ${show(missing)}`);
  });
  cases['DEF-003'] = () => withClient(async (c) => eq('arrow_ipc', await one(c, 'SHOW pdb.wire_result_format'), 'server-recorded result format'));
  cases['DEF-004'] = async () => {
    throw new Skip('the SDK exposes no client-side record of the negotiated protocol / capabilities to compare (DEF-001 / DEF-002 check the server record)');
  };
  cases['DEF-005'] = async () => {
    const c = new PdbWireClient({ host: HOST });
    try {
      eq(defaultPort(), (c as unknown as { opts: { port: number } }).opts.port, 'SDK default port vs server default port');
      if (port() !== defaultPort()) throw new Skip(`default-port constant checked; the server runs on ${port()}, not the default ${defaultPort()}`);
      eq('pdb-native-v2', await one(c, 'SHOW pdb.wire_protocol'), 'port-less connect protocol');
    } finally { await c.close(); }
  };
  cases['DEF-006'] = () => withClient(async (c) => {
    const hostile = "o'clock; DROP TABLE x -- ";
    for (let i = 0; i < 10; i++) eq(hostile, await one(c, "SELECT $1::text || ''", [hostile]), `bound text param, execution ${i}`);
    const cached = (c as unknown as { pool: { idle: Array<{ stmts: Map<string, unknown> }> } }).pool.idle.some((k) => k.stmts.has("SELECT $1::text || ''"));
    check(cached, 'the parameterised statement was not held as a server-side prepared statement');
  }, 1);

  // ── SQL ──
  cases['SQL-PREP-001'] = () => withClient(async (c) => {
    await c.queryArrays('DROP TABLE IF EXISTS conf_prep1');
    await c.queryArrays('CREATE TABLE conf_prep1 (id int8 PRIMARY KEY, v text)');
    await c.queryArrays("INSERT INTO conf_prep1 SELECT g, 'v' || g FROM generate_series(1, 50) g");
    for (let pass = 0; pass < 2; pass++) {
      for (let k = 1; k <= 50; k++) {
        const i = pass === 0 ? k : 51 - k;
        eq([[`v${i}`]], (await c.queryArrays('SELECT v FROM conf_prep1 WHERE id = $1', [i])).rows, `id ${i}`);
      }
    }
  }, 1);
  cases['SQL-PREP-002'] = () => withClient(async (c) => {
    await c.queryArrays('DROP TABLE IF EXISTS conf_prep2');
    await c.queryArrays('CREATE TABLE conf_prep2 (id int8 PRIMARY KEY, a text)');
    await c.queryArrays("INSERT INTO conf_prep2 VALUES (1, 'x')");
    for (let i = 0; i < 6; i++) eq(2, (await c.queryArrays('SELECT * FROM conf_prep2 WHERE id = $1', [1])).fields.length, 'width before ALTER');
    await c.queryArrays('ALTER TABLE conf_prep2 ADD COLUMN b int8');
    await c.queryArrays('UPDATE conf_prep2 SET b = 5');
    let r: RawQueryResult;
    try { r = await c.queryArrays('SELECT * FROM conf_prep2 WHERE id = $1', [1]); } catch (e) {
      check(['0A000', '26000'].includes(sqlstate(e) ?? ''), `after ALTER: unexpected ${(e as Error).message} [${sqlstate(e)}]`);
      return;
    }
    eq(3, r.fields.length, 'width after ALTER (stale describe?)');
    eq(5, r.rows[0][2], 'column b after ALTER');
  }, 1);
  cases['SQL-BATCH-001'] = () => withClient(async (c) => {
    await c.queryArrays('DROP TABLE IF EXISTS conf_b1');
    await c.queryArrays('CREATE TABLE conf_b1 (id int8 PRIMARY KEY, v text)');
    const r = await c.executeBatch('INSERT INTO conf_b1 VALUES ($1, $2)', Array.from({ length: 1000 }, (_, k) => [k + 1, `r${k + 1}`]));
    eq(1000, r.rowCounts.length, 'per-row count entries');
    const bad = r.rowCounts.map((n, k) => [n, k]).filter(([n]) => n !== 1);
    check(bad.length === 0, `counts not 1 at ${show(bad.slice(0, 5))}`);
    eq(1000, await one(c, 'SELECT count(*) FROM conf_b1'), 'rows');
    eq(500500, Number(await one(c, 'SELECT sum(id) FROM conf_b1')), 'sum');
  });
  cases['SQL-BATCH-002'] = () => withClient(async (c) => {
    await c.queryArrays('DROP TABLE IF EXISTS conf_b2');
    await c.queryArrays('CREATE TABLE conf_b2 (id int8 PRIMARY KEY)');
    const err = await c.transaction((tx) => tx.executeBatch('INSERT INTO conf_b2 VALUES ($1)', [[1], [2], [1], [3]])).catch((e) => e);
    check(err instanceof BatchExecuteError, `batch with a duplicate key: ${show(err?.message ?? 'succeeded')}`);
    const msg = (err as BatchExecuteError).errors?.[0]?.message ?? err.message;
    check(/duplicate key|23505/.test(String(msg)), `batch failure entry is not 23505: ${show(msg)}`);
    check((err as BatchExecuteError).errors[0].row === 2, `failing entry index ${(err as BatchExecuteError).errors[0].row}, expected 2`);
    eq(0, await one(c, 'SELECT count(*) FROM conf_b2'), 'rows after rollback');
  });
  cases['SQL-STREAM-001'] = async () => {
    throw new Skip('the SDK offers no bounded-memory streaming surface: query() and stream() materialize the whole result (documented in the SDK README "Scope")');
  };
  cases['SQL-MULTI-001'] = () => withClient(async (c) => {
    await c.queryArrays('DROP TABLE IF EXISTS conf_m1');
    await c.queryArrays('CREATE TABLE conf_m1 (id int8 PRIMARY KEY)');
    const err = await c.queryArrays('INSERT INTO conf_m1 VALUES (1); INSERT INTO conf_m1 VALUES (1)').catch((e) => e);
    check(err instanceof Error, 'multi-statement with a duplicate key succeeded');
    eq('23505', sqlstate(err), `multi-statement failure SQLSTATE (${err.message})`);
    eq(0, await one(c, 'SELECT count(*) FROM conf_m1'), 'rows (implicit transaction must roll back)');
  });
  cases['SQL-MULTI-002'] = () => withClient(async (c) => {
    await c.queryArrays('DROP TABLE IF EXISTS conf_m2');
    await c.queryArrays('CREATE TABLE conf_m2 (id int8 PRIMARY KEY)');
    eq(2, await one(c, 'INSERT INTO conf_m2 VALUES (1); INSERT INTO conf_m2 VALUES (2); SELECT count(*) FROM conf_m2'), 'final SELECT result');
  });
  cases['SQL-TXN-005'] = async () => {
    const a = client(1);
    const pool = new ParticleDB({ host: HOST, port: port(), poolSize: 4 });
    try {
      await a.queryArrays('DROP TABLE IF EXISTS conf_x5');
      await a.queryArrays('CREATE TABLE conf_x5 (id int8 PRIMARY KEY, v int8)');
      await a.queryArrays('INSERT INTO conf_x5 VALUES (1, 0)');
      for (let i = 1; i <= 200; i++) {
        eq(1, (await a.queryArrays('UPDATE conf_x5 SET v = $1 WHERE id = 1', [i])).rowCount, `update count ${i}`);
        eq(i, await one(a, 'SELECT v FROM conf_x5 WHERE id = 1'), `same-connection read ${i}`);
        eq(i, (await pool.queryOne<{ v: number }>('SELECT v FROM conf_x5 WHERE id = $1', [1]))?.v, `pooled-connection read ${i}`);
      }
    } finally { await a.close(); await pool.close(); }
  };
  cases['SQL-CANCEL-001'] = async () => {
    throw new Skip('the SDK offers no statement cancel (protocol 1 has no per-stream reset; documented in the SDK README "Scope")');
  };
  cases['SQL-TIMEOUT-001'] = async () => {
    // The SDK's own setter (timeoutMs) is a client-side deadline that closes
    // the connection (outcome unknown), not a server 57014; only the SET path
    // of this case applies.
    const c = client(1);
    const conn = await c.reserve();
    try {
      await conn.queryArrays("SET statement_timeout = '500ms'");
      const t0 = Date.now();
      const err = await conn.queryArrays('SELECT pg_sleep(30)').catch((e) => e);
      check(err instanceof Error, 'statement_timeout statement completed');
      eq('57014', sqlstate(err), `statement_timeout SQLSTATE (${err.message})`);
      check(Date.now() - t0 < 6000, 'statement_timeout took > 6 s');
      await conn.queryArrays('RESET statement_timeout');
      eq(1, (await conn.queryArrays('SELECT 1')).rows[0][0], 'connection usable after statement_timeout');
    } finally { await conn.release(); await c.close(); }
  };

  // ── CONCURRENCY ──
  for (const id of ['CONC-001', 'CONC-002', 'CONC-003', 'CONC-005']) {
    cases[id] = async () => { throw new Skip(NO_MUX); };
  }
  cases['CONC-004'] = async () => {
    const z = client(1);
    try {
      await z.queryArrays('DROP TABLE IF EXISTS conf_c4');
      await z.queryArrays('CREATE TABLE conf_c4 (id int8 PRIMARY KEY, owner text)');
      const a = client(1);
      const b = client(1);
      try {
        const view = async (tx: { queryArrays: PdbWireClient['queryArrays'] }) =>
          (await tx.queryArrays('SELECT owner FROM conf_c4 ORDER BY id')).rows.map((r) => r[0]).join('');
        // Both transactions stay open until both have read: under READ
        // COMMITTED (the session default) a statement sees rows committed
        // before it starts, so letting one side commit while the other is
        // still reading would make the expected view depend on timing.
        let releaseA!: () => void;
        const gate = new Promise<void>((r) => (releaseA = r));
        let readA!: () => void;
        const aRead = new Promise<void>((r) => (readA = r));
        let readB!: () => void;
        const bRead = new Promise<void>((r) => (readB = r));
        const ta = a.transaction(async (tx) => {
          await tx.queryArrays("INSERT INTO conf_c4 VALUES (1, 'a')");
          await gate;
          let views: string[];
          try { views = await Promise.all(Array.from({ length: 8 }, () => view(tx))); }
          finally { readA(); }
          await bRead;
          views.forEach((v, t) => eq('a', v, `concurrent statement ${t} inside A's transaction`));
        });
        const tb = b.transaction(async (tx) => {
          await tx.queryArrays("INSERT INTO conf_c4 VALUES (2, 'b')");
          releaseA();
          let views: string[];
          try { views = await Promise.all(Array.from({ length: 8 }, () => view(tx))); }
          finally { readB(); }
          await aRead;
          views.forEach((v, t) => eq('b', v, `concurrent statement ${t} inside B's transaction`));
        });
        await Promise.all([ta, tb]);
      } finally { await a.close(); await b.close(); }
      eq('ab', await one(z, "SELECT string_agg(owner, '' ORDER BY id) FROM conf_c4"), 'after both commits');
    } finally { await z.close(); }
  };

  // ── KV parity ──
  const facade = () => new ParticleDB({ host: HOST, port: port(), poolSize: 2 });
  const withKV = async (fn: (db: ParticleDB, r: Resp) => Promise<void>) => {
    const db = facade();
    const r = await Resp.open();
    try { await fn(db, r); } finally { r.close(); await db.close(); }
  };
  cases['KV-001'] = () => withKV(async (db, r) => {
    await db.kv.set('conf:kv1', 'hello wörld');
    eq('hello wörld', await r.str('GET', 'conf:kv1'), 'RESP GET of SDK SET');
    await r.cmd('SET', 'conf:kv1b', 'from resp');
    eq('from resp', await db.kv.get('conf:kv1b'), 'SDK GET of RESP SET');
  });
  cases['KV-002'] = () => withKV(async (db, r) => {
    eq(null, await db.kv.get('conf:kv2-absent'), 'GET of an absent key');
    await db.kv.set('conf:kv2', 'x');
    check(await db.kv.delete('conf:kv2'), 'DEL of a present key reported not-deleted');
    eq(0, await r.num('EXISTS', 'conf:kv2'), 'RESP EXISTS after SDK DEL');
    check(!(await db.kv.exists('conf:kv2')), 'SDK exists after DEL');
  });
  cases['KV-003'] = () => withKV(async (db, r) => {
    await r.cmd('DEL', 'conf:kv3');
    eq(1, await db.kv.incr('conf:kv3'), 'SDK INCR of absent');
    eq(11, await r.num('INCRBY', 'conf:kv3', '10'), 'RESP INCRBY');
    eq(12, await db.kv.incr('conf:kv3'), 'SDK INCR');
    eq('12', await r.str('GET', 'conf:kv3'), 'RESP GET');
  });
  cases['KV-004'] = () => withKV(async (db, r) => {
    await db.kv.set('conf:kv4', 'v', 100);
    const ttl = await r.num('TTL', 'conf:kv4');
    check(ttl >= 99 && ttl <= 100, `RESP TTL after SDK set(ttl=100): ${ttl}`);
    await r.cmd('SET', 'conf:kv4b', 'w', 'EX', '200');
    const t2 = await db.kv.ttl('conf:kv4b');
    check(t2 !== null && t2 >= 199 && t2 <= 200, `SDK TTL after RESP EX 200: ${t2}`);
  });
  cases['KV-005'] = () => withKV(async (db, r) => {
    await db.kv.set('conf:kv5', 'v');
    eq(-1, await db.kv.ttl('conf:kv5'), 'SDK TTL of a key with none');
    eq(-1, await r.num('TTL', 'conf:kv5'), 'RESP TTL of a key with none');
    eq(-2, await db.kv.ttl('conf:kv5-absent'), 'SDK TTL of a missing key');
    eq(-2, await r.num('TTL', 'conf:kv5-absent'), 'RESP TTL of a missing key');
  });
  cases['KV-006'] = () => withKV(async (db, r) => {
    await r.cmd('SET', 'conf:kv6', 'a', 'EX', '300');
    await db.kv.set('conf:kv6', 'b');
    eq(-1, await r.num('TTL', 'conf:kv6'), 'RESP TTL after SDK plain SET');
    eq('b', await r.str('GET', 'conf:kv6'), 'RESP GET');
  });
  cases['KV-007'] = () => withKV(async (db, r) => {
    await db.kv.set('conf:kv7', 'v', 1);
    await sleep(2200);
    eq(null, await db.kv.get('conf:kv7'), 'SDK GET of an expired key');
    eq(0, await r.num('EXISTS', 'conf:kv7'), 'RESP EXISTS of an expired key');
    check(!(await db.kv.exists('conf:kv7')), 'SDK exists of an expired key');
  });
  cases['KV-008'] = async () => { throw new Skip('the SDK KV surface is string-valued (kv.set/get take and return text); it offers no binary-safe value'); };
  cases['KV-009'] = () => withKV(async (db, r) => {
    const bigv = 'x'.repeat(1 << 20);
    await db.kv.set('conf:kv9', bigv);
    eq(1048576, await r.num('STRLEN', 'conf:kv9'), 'RESP STRLEN');
    eq(bigv, await r.str('GET', 'conf:kv9'), 'RESP GET of 1 MiB');
  });
  cases['KV-010'] = () => withKV(async (db, r) => {
    await r.cmd('DEL', 'conf:kv10-absent');
    await db.kv.mset({ 'conf:kv10a': '1', 'conf:kv10b': '2' });
    const m = (await r.cmd('MGET', 'conf:kv10a', 'conf:kv10b', 'conf:kv10-absent')) as Array<Buffer | null>;
    eq(['1', '2', null], m.map((x) => (x === null ? null : x.toString())), 'RESP MGET');
    eq(['1', null, '2'], await db.kv.mget(['conf:kv10a', 'conf:kv10-absent', 'conf:kv10b']), 'SDK mget');
  });
  cases['KV-011'] = () => withKV(async (db, r) => {
    await r.cmd('DEL', 'conf:h', 'conf:l', 'conf:s', 'conf:z');
    await db.query('SELECT HASH_SET($1, $2, $3)', ['conf:h', 'f', 'v']);
    eq('v', await r.str('HGET', 'conf:h', 'f'), 'RESP HGET of SDK HASH_SET');
    await db.query('SELECT LIST_PUSH($1, $2)', ['conf:l', 'a']);
    await db.query('SELECT LIST_PUSH($1, $2)', ['conf:l', 'b']);
    eq(2, ((await r.cmd('LRANGE', 'conf:l', '0', '-1')) as unknown[]).length, 'RESP LRANGE length');
    await db.query('SELECT SET_ADD($1, $2)', ['conf:s', 'm']);
    eq(1, await r.num('SISMEMBER', 'conf:s', 'm'), 'RESP SISMEMBER of SDK SET_ADD');
    await db.query('SELECT ZSET_ADD($1, $2, $3)', ['conf:z', 1.5, 'm']);
    eq('1.5', await r.str('ZSCORE', 'conf:z', 'm'), 'RESP ZSCORE of SDK ZSET_ADD');
  });
  cases['KV-012'] = async () => {
    throw new Skip('the SDK KV API lives on the client, not on a transaction, and has no native KV frame; a SQL KV_SET issued inside a block is part of that transaction');
  };

  // ── DURABILITY ──
  const crashRestart = () => { control('kill9'); control('restart'); };
  cases['DUR-001'] = async () => {
    await withClient(async (c) => {
      await c.queryArrays('DROP TABLE IF EXISTS conf_dur1');
      await c.queryArrays('CREATE TABLE conf_dur1 (id int8 PRIMARY KEY, v text)');
      for (let i = 1; i <= 500; i++) await c.queryArrays("INSERT INTO conf_dur1 VALUES ($1, 'v')", [i]);
    });
    crashRestart();
    await withClient(async (c) => {
      eq(500, await one(c, 'SELECT count(*) FROM conf_dur1'), 'acked rows after kill -9');
      eq(125250, Number(await one(c, 'SELECT sum(id) FROM conf_dur1')), 'acked ids after kill -9');
    });
  };
  cases['DUR-002'] = async () => {
    await withClient(async (c) => {
      await c.queryArrays('DROP TABLE IF EXISTS conf_dur2');
      await c.queryArrays('CREATE TABLE conf_dur2 (id int8 PRIMARY KEY, v int8)');
      await c.transaction(async (tx) => {
        await tx.queryArrays('INSERT INTO conf_dur2 SELECT g, 1 FROM generate_series(1, 100) g');
        await tx.queryArrays('UPDATE conf_dur2 SET v = 101 WHERE id = 7');
      });
    });
    crashRestart();
    await withClient(async (c) => {
      eq(100, await one(c, 'SELECT count(*) FROM conf_dur2'), 'rows of an acked COMMIT after kill -9');
      eq(200, Number(await one(c, 'SELECT sum(v) FROM conf_dur2')), "the COMMIT's UPDATE after kill -9");
    });
  };
  cases['DUR-003'] = async () => {
    await withClient(async (c) => {
      await c.queryArrays('DROP TABLE IF EXISTS conf_dur3');
      await c.queryArrays('CREATE TABLE conf_dur3 (id int8 PRIMARY KEY)');
      await c.executeBatch('INSERT INTO conf_dur3 VALUES ($1)', Array.from({ length: 1000 }, (_, k) => [k + 1]));
    });
    crashRestart();
    await withClient(async (c) => eq(1000, await one(c, 'SELECT count(*) FROM conf_dur3'), 'acked batch rows after kill -9'));
  };
  cases['DUR-004'] = async () => {
    const db = facade();
    try {
      await db.kv.set('conf:dur4', 'durable');
      await db.kv.delete('conf:dur4n');
      for (let i = 0; i < 3; i++) await db.kv.incr('conf:dur4n');
    } finally { await db.close(); }
    crashRestart();
    await withKV(async (db2, r) => {
      eq('durable', await db2.kv.get('conf:dur4'), 'SDK GET after kill -9');
      eq('3', await r.str('GET', 'conf:dur4n'), 'RESP GET of the counter after kill -9');
    });
  };
  cases['DUR-005'] = async () => {
    const a = client(1);
    try {
      eq(1, await one(a, 'SELECT 1'), 'before the kill');
      control('kill9');
      const t0 = Date.now();
      const err = await a.queryArrays('SELECT 1').catch((e) => e);
      check(err instanceof Error, 'statement on a connection to a killed server succeeded');
      check(err.name === 'ConnectionError', `connection loss surfaced as ${err.name}: ${err.message} (expected ConnectionError, SQLSTATE class 08)`);
      check(Date.now() - t0 < 10_000, 'connection loss took > 10 s to surface');
    } finally {
      await a.close().catch(() => undefined);
      control('restart');
    }
    await withClient(async (b) => eq(1, await one(b, 'SELECT 1'), 'new connection after restart'));
  };
  return cases;
}

// ---------------------------------------------------------------------------
// Run loop
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  const dir = env('PDB_CONF_CASES_DIR');
  const all: Record<string, any> = {};
  for (const name of readdirSync(dir).sort()) {
    if (name.endsWith('.json')) for (const c of JSON.parse(readFileSync(join(dir, name), 'utf8')).cases) all[c.id] = c;
  }
  const ids = readFileSync(env('PDB_CONF_IDS'), 'utf8').split('\n').map((l) => l.trim()).filter(Boolean);
  const only = process.env.PDB_CONF_ONLY ? new Set(process.env.PDB_CONF_ONLY.split(',')) : null;
  const custom = customs(all);
  const results: Record<string, Record<string, unknown>> = {};
  const counts = { pass: 0, fail: 0, skip: 0 };
  for (const id of ids) {
    const c = all[id];
    let r: Record<string, unknown>;
    const t0 = Date.now();
    if (!c) r = { status: 'fail', detail: 'the runner listed a case id the case files do not define' };
    else if (only && !only.has(id)) r = { status: 'skip', reason: 'PDB_CONF_ONLY excluded it (debug run)' };
    else {
      const body = c.kind === 'script'
        ? runScript(c)
        : custom[id] ? custom[id]() : Promise.reject(new Fail('adapter has no implementation of this custom case'));
      let timer: NodeJS.Timeout | undefined;
      const timeout = new Promise<never>((_, rej) => { timer = setTimeout(() => rej(new Fail(`case did not finish within ${CASE_TIMEOUT_MS / 1000} s`)), CASE_TIMEOUT_MS); });
      try {
        await Promise.race([body, timeout]);
        r = { status: 'pass' };
      } catch (e) {
        if (e instanceof Skip) r = { status: 'skip', reason: e.message };
        else {
          const st = sqlstate(e);
          let detail = (e instanceof Fail ? '' : `${(e as Error).name}: `) + (e as Error).message;
          if (st && !(e instanceof Fail)) detail += ` [SQLSTATE ${st}]`;
          r = { status: 'fail', detail: detail || 'error' };
        }
      } finally { clearTimeout(timer); }
      r.ms = Date.now() - t0;
    }
    counts[r.status as keyof typeof counts]++;
    console.log(`${id.padEnd(16)} ${String(r.status).padEnd(4)} ${r.detail ?? r.reason ?? ''}`);
    results[id] = r;
  }
  writeFileSync(env('PDB_CONF_RESULTS'), JSON.stringify({ sdk: 'typescript', sdk_version: SDK_VERSION, mode: mode(), results }, null, 1) + '\n');
  console.log(`typescript: ${counts.pass} pass, ${counts.fail} fail, ${counts.skip} skip`);
  return counts.fail === 0 ? 0 : 1;
}

main().then((rc) => process.exit(rc), (e) => { console.error(e); process.exit(2); });
