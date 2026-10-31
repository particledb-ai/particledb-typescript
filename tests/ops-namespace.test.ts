/**
 * The three namespace Ops methods cannot succeed, and now say so.
 *
 * `createNamespace` / `setNamespace` / `dropNamespace` used to emit
 * `CREATE NAMESPACE` / `SET NAMESPACE` / `DROP NAMESPACE IF EXISTS`. No parser
 * in the engine accepts any of those verbs on any wire, so every call was
 * refused, while the JSDoc described a row count on success - an outcome that
 * could not occur.
 *
 * What is pinned:
 *
 *  - the call rejects, never resolves;
 *  - it rejects WITHOUT sending SQL - the pool used here throws if `query` is
 *    ever called, so a rejection carrying the namespace message (rather than
 *    the pool's own) proves nothing was sent. That is what separates "fails
 *    fast with an explanation" from "sends dead SQL and relays a parse error";
 *  - the message names the verb and the cause.
 *
 * Reverting the fix turns these red: the rejection would carry the pool's
 * sentinel message instead.
 *
 * Run with:  npx tsx --test tests/ops-namespace.test.ts
 * (`node --test` alone cannot load this SDK - src/ops.ts uses TypeScript
 * parameter properties, which node's strip-only mode rejects.)
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { ExtendedOps } from '../src/ops';

const SENTINEL = 'POOL_WAS_CALLED';

/** A pool stand-in that fails the test if any SQL reaches it. */
function refusingPool() {
  return {
    query(sql: string): Promise<never> {
      throw new Error(`${SENTINEL}: ${sql}`);
    },
  } as any;
}

const CASES: Array<{ name: string; verb: string; run: (o: ExtendedOps) => Promise<unknown> }> = [
  {
    name: 'createNamespace',
    verb: 'CREATE NAMESPACE',
    run: (o) => o.createNamespace('tenant_acme'),
  },
  {
    name: 'createNamespace with quotas',
    verb: 'CREATE NAMESPACE',
    run: (o) => o.createNamespace('tenant_acme', { max_tables: 100 }),
  },
  {
    name: 'setNamespace',
    verb: 'SET NAMESPACE',
    run: (o) => o.setNamespace('tenant_acme'),
  },
  {
    name: 'dropNamespace',
    verb: 'DROP NAMESPACE',
    run: (o) => o.dropNamespace('tenant_acme'),
  },
];

// Positive control for the probe: a zero-send claim is worthless unless the
// instrument can register a send, so prove the refusing pool really does fire
// for a method that contacts the server.
test('refusing pool detects a send', async () => {
  const ops = new ExtendedOps(refusingPool());
  await assert.rejects(
    () => ops.refreshMaterializedView('mv_users'),
    (err: Error) => err.message.includes(SENTINEL),
    'the probe cannot distinguish "sent nothing" from "sent something"'
  );
});

for (const { name, verb, run } of CASES) {
  test(`${name} rejects without sending SQL`, async () => {
    const ops = new ExtendedOps(refusingPool());

    await assert.rejects(run(ops), (err: unknown) => {
      assert.ok(err instanceof Error, 'expected an Error');
      const msg = err.message;
      assert.ok(
        !msg.includes(SENTINEL),
        `SQL reached the server; the method must fail fast. Got: ${msg}`
      );
      assert.match(msg, new RegExp(verb));
      assert.match(msg, /not implemented by any ParticleDB server/);
      assert.match(msg, /can never succeed/);
      assert.match(msg, /separate server per tenant/);
      assert.match(msg, /databases-and-schemas/);
      return true;
    });
  });
}
