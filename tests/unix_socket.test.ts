/**
 * Unix-domain-socket selection for the native wire client.
 *
 * Unit part (no server): which hosts try the socket, the path convention
 * (`<unixSocketDir>/.s.PDB.<port>`, the server's `--unix-socket-dir`), the
 * `transport: 'tcp'` opt-out, and the TCP fallback when the file is absent.
 *
 * Live part (runs only when PDB_NATIVE_PORT is set; a --no-auth server with
 * its default socket in /tmp): `health().transport` reports the socket kind
 * the pooled connection really used.
 *
 *   node --import tsx --test tests/unix_socket.test.ts
 *   PDB_NATIVE_PORT=7173 node --import tsx --test tests/unix_socket.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';

import {
  DEFAULT_UNIX_SOCKET_DIR,
  ParticleDB,
  PdbWireClient,
  isLocalHost,
  nativeEndpoints,
  unixSocketPath,
} from '../src';

const PORT = process.env.PDB_NATIVE_PORT;

describe('unix socket selection (unit)', () => {
  it('local host literals, and no DNS for names', () => {
    for (const h of ['localhost', 'LOCALHOST', '127.0.0.1', '127.9.9.9', '::1', '[::1]']) {
      assert.ok(isLocalHost(h), h);
    }
    for (const h of ['10.0.0.7', 'db.example.com', '192.0.2.1', '::2']) {
      assert.ok(!isLocalHost(h), h);
    }
  });

  it('path convention matches the server', () => {
    assert.equal(DEFAULT_UNIX_SOCKET_DIR, '/tmp');
    assert.equal(unixSocketPath(7173), path.join('/tmp', '.s.PDB.7173'));
    assert.equal(unixSocketPath(7001, '/run/pdb'), path.join('/run/pdb', '.s.PDB.7001'));
  });

  it('auto: socket first for a local host only when the file exists', () => {
    const yes = () => true;
    const no = () => false;
    assert.deepEqual(nativeEndpoints('127.0.0.1', 7001, 'auto', '/run/pdb', yes), [
      { kind: 'unix', path: path.join('/run/pdb', '.s.PDB.7001'), fallback: true },
      { kind: 'tcp', host: '127.0.0.1', port: 7001, fallback: false },
    ]);
    assert.deepEqual(nativeEndpoints('[::1]', 7001, 'auto', '/tmp', yes)[1], {
      kind: 'tcp', host: '::1', port: 7001, fallback: false,
    });
    assert.deepEqual(nativeEndpoints('127.0.0.1', 7001, 'auto', '/tmp', no), [
      { kind: 'tcp', host: '127.0.0.1', port: 7001, fallback: false },
    ]);
    assert.deepEqual(nativeEndpoints('10.0.0.7', 7001, 'auto', '/tmp', yes), [
      { kind: 'tcp', host: '10.0.0.7', port: 7001, fallback: false },
    ]);
  });

  it("'tcp' opts out; 'unix' forces the socket", () => {
    const yes = () => true;
    assert.deepEqual(nativeEndpoints('localhost', 7001, 'tcp', '/tmp', yes), [
      { kind: 'tcp', host: 'localhost', port: 7001, fallback: false },
    ]);
    assert.deepEqual(nativeEndpoints('db.example.com', 7001, 'unix', '/x', () => false), [
      { kind: 'unix', path: path.join('/x', '.s.PDB.7001'), fallback: false },
    ]);
  });

  it('pdb:// URL: ?transport= is validated', async () => {
    await assert.rejects(ParticleDB.connect('pdb://127.0.0.1:1/?transport=bogus'), /invalid transport/);
  });
});

describe('unix socket selection (live)', { skip: PORT ? false : 'PDB_NATIVE_PORT not set' }, () => {
  const port = Number(PORT);

  it('loopback rides the server socket; tcp / another dir ride TCP', async (t) => {
    if (!fs.existsSync(unixSocketPath(port))) {
      t.skip(`server has no socket at ${unixSocketPath(port)}`);
      return;
    }
    const cases: Array<[Record<string, unknown>, 'unix' | 'tcp']> = [
      [{}, 'unix'],
      [{ host: 'localhost' }, 'unix'],
      [{ transport: 'tcp' }, 'tcp'],
      [{ unixSocketDir: '/nonexistent' }, 'tcp'],
      [{ transport: 'unix' }, 'unix'],
    ];
    for (const [extra, want] of cases) {
      const db = new PdbWireClient({ host: '127.0.0.1', port, poolSize: 1, ...extra });
      try {
        const h = await db.health();
        assert.ok(h.connected, JSON.stringify(extra));
        assert.equal(h.transport, want, JSON.stringify(extra));
        const rows = await db.query('SELECT 7 AS v');
        assert.equal(Number(rows[0].v), 7);
      } finally {
        await db.close();
      }
    }
  });
});
