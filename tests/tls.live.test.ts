import { it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { ParticleDB, PdbWireClient } from '../src';

const port = Number(process.env.PDB_TLS_PORT);
const ca = process.env.PDB_TLS_CA ? readFileSync(process.env.PDB_TLS_CA) : undefined;
const skip = !port || !ca;

it('verified native TLS supports concurrent queries and a pinned transaction', {skip}, async () => {
  const db = await ParticleDB.connect({host: 'localhost', port, ssl: {ca}, poolSize: 2});
  const table = `ts_tls_${process.pid}`;
  try {
    const rows = await Promise.all(Array.from({length: 20}, () => db.queryOne('SELECT 42 AS answer')));
    assert.ok(rows.every(row => row?.answer === 42));
    await db.execute(`CREATE TABLE ${table} (id BIGINT PRIMARY KEY, name TEXT)`);
    await db.transaction(async tx => { await tx.execute(`INSERT INTO ${table} VALUES ($1, $2)`, [1, 'first']); });
    await assert.rejects(db.transaction(async tx => {
      await tx.execute(`INSERT INTO ${table} VALUES ($1, $2)`, [2, 'rolled back']);
      throw new Error('stop');
    }), /stop/);
    assert.equal((await db.queryOne(`SELECT count(*) AS n FROM ${table}`))?.n, 1);
  } finally {
    await db.execute(`DROP TABLE IF EXISTS ${table}`).catch(() => undefined);
    await db.close();
  }
});

it('native TLS rejects an untrusted certificate and the wrong hostname', {skip}, async () => {
  for (const config of [{host: 'localhost', ssl: true}, {host: '127.0.0.1', ssl: {ca}}]) {
    const db = new PdbWireClient({...config, port, maxRetries: 0, timeoutMs: 3000});
    try { await assert.rejects(db.query('SELECT 1'), /certificate|hostname|altname|self.signed/i); }
    finally { await db.close(); }
  }
});

it('native TLS refuses a plaintext server', {skip: !process.env.PDB_NATIVE_PORT}, async () => {
  const db = new PdbWireClient({host: '127.0.0.1', port: Number(process.env.PDB_NATIVE_PORT), ssl: true, maxRetries: 0});
  try { await assert.rejects(db.query('SELECT 1'), /TLS|plaintext/i); }
  finally { await db.close(); }
});

it('invalid pool and timeout settings fail before connecting', () => {
  for (const opts of [{poolSize: 0}, {port: 65536}, {timeoutMs: NaN}, {poolSize: 1.5}]) {
    assert.throws(() => new PdbWireClient(opts), /Invalid/);
  }
  assert.throws(() => new PdbWireClient({ssl: true, transport: 'unix'}), /TLS requires TCP/);
});

it('a timed out pool waiter does not consume the next returned connection', {skip: !process.env.PDB_NATIVE_PORT}, async () => {
  const db = new PdbWireClient({host: '127.0.0.1', port: Number(process.env.PDB_NATIVE_PORT), poolSize: 1, timeoutMs: 100, maxRetries: 0});
  let entered!: () => void;
  const ready = new Promise<void>(resolve => { entered = resolve; });
  const owner = db.transaction(async () => {
    entered();
    await new Promise(resolve => setTimeout(resolve, 250));
  });
  try {
    await ready;
    await assert.rejects(db.query('SELECT 1'), /Pool wait timeout/);
    await owner;
    assert.deepEqual(await db.query('SELECT 42 AS answer'), [{answer: 42}]);
  } finally { await db.close(); }
});

it('transaction queries are ordered and the context expires after completion', {skip: !process.env.PDB_NATIVE_PORT}, async () => {
  const db = new PdbWireClient({host: '127.0.0.1', port: Number(process.env.PDB_NATIVE_PORT), poolSize: 1});
  let saved: any;
  try {
    await db.transaction(async tx => {
      saved = tx;
      const results = await Promise.all([tx.query('SELECT 1 AS n'), tx.query('SELECT 2 AS n')]);
      assert.deepEqual(results, [[{n: 1}], [{n: 2}]]);
    });
    await assert.rejects(saved.query('SELECT 3'), /Transaction has finished/);
    assert.deepEqual(await db.query('SELECT 4 AS n'), [{n: 4}]);
  } finally { await db.close(); }
});

it('a transaction statement timeout closes the connection without replaying it', {skip: !process.env.PDB_NATIVE_PORT}, async () => {
  const db = new PdbWireClient({host: '127.0.0.1', port: Number(process.env.PDB_NATIVE_PORT), timeoutMs: 100, maxRetries: 0});
  try {
    await assert.rejects(db.transaction(tx => tx.query('SELECT pg_sleep(1)')), /timed out|closed/i);
    assert.deepEqual(await db.query('SELECT 7 AS n'), [{n: 7}]);
  } finally { await db.close(); }
});

it('connection options and logged configuration cannot silently expose credentials', async () => {
  const {inspect} = await import('node:util');
  const db = new PdbWireClient({user: 'app', password: 'private-credential'});
  try {
    assert.ok(!inspect(db, {depth: 10}).includes('private-credential'));
    assert.ok(!JSON.stringify(db).includes('private-credential'));
    await assert.rejects(ParticleDB.connect('pdb://localhost?sslmode=verify-full'), /Unsupported connection option/);
    await assert.rejects(ParticleDB.connect('pdb://localhost?ssl=verify-full'), /ssl must be/);
  } finally { await db.close(); }
});

it('native authentication accepts valid credentials and rejects a wrong password', {skip: !process.env.PDB_AUTH_PORT}, async () => {
  for (const valid of [true, false]) {
    const db = new PdbWireClient({host: '127.0.0.1', port: Number(process.env.PDB_AUTH_PORT),
      user: process.env.PDB_AUTH_USER, password: valid ? process.env.PDB_AUTH_PASS : 'wrong-test-password', maxRetries: 0});
    try {
      if (valid) assert.deepEqual(await db.query('SELECT 42 AS n'), [{n: 42}]);
      else await assert.rejects(db.query('SELECT 42'), /password|authentication|credential/i);
    } finally { await db.close(); }
  }
});

it('pipeline helpers use supported DDL and reject invalid identifiers', {skip: !process.env.PDB_NATIVE_PORT}, async () => {
  const db = await ParticleDB.connect({host: '127.0.0.1', port: Number(process.env.PDB_NATIVE_PORT)});
  const name = `ts_pipeline_${process.pid}`;
  try {
    const config = {table: 'documents', vectorCol: 'embedding', textCol: 'content'};
    await db.rag.createPipeline(name, config);
    await db.execute(`DROP RAG PIPELINE ${name}`);
    await assert.rejects(db.rag.createPipeline('bad; SELECT 1', config), /identifier/);
    await assert.rejects(db.rag.createPipeline(name, {...config, mode: 'rerank'}), /mode/);
    await assert.rejects(db.rag.createPipeline(name, {...config, k: 0}), /positive/);
  } finally { await db.close(); }
});
