/**
 * Unit tests for the dependency-free Arrow IPC codec (src/arrow_ipc.ts).
 *
 * - The DECODER reads a stream written by an independent producer (pyarrow
 *   23, embedded as hex below) covering every type the server ships to this
 *   client, with nulls.
 * - The ENCODER output is byte-checked for the IPC framing invariants
 *   (continuation markers, 8-byte aligned metadata and body, end-of-stream)
 *   and round-tripped through the decoder for int64 / float64 / utf8 / bool /
 *   binary with nulls.
 * - When `python3` with `pyarrow` is on PATH, pyarrow reads the encoder's
 *   output too (independent reader); otherwise that case is skipped.
 *
 *   node --import tsx --test tests/arrow_ipc.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

import { decodeArrowStream, encodeArrowStream, inferParamType, normalizeParam, ParamColumn } from '../src/arrow_ipc';

/** pyarrow 23 `ipc.new_stream` output for the table described in the decoder test. */
const PYARROW_GOLDEN = Buffer.from(
  'ffffffff400200001000000000000a000c000600050008000a000000000104000c0000000800080000000400080000000400' +
  '00000a000000d801000098010000680100003c01000014010000ec000000b4000000880000005c0000000400000060feffff' +
  '0000010c1400000018000000040000000100000010000000010000006c000000e4feffff88feffff00000103100000001800' +
  '00000400000000000000040000006974656d00000000e6feffff00000100b4feffff00000108100000001400000004000000' +
  '0000000002000000647400000effffff00000000dcfeffff0000010a10000000140000000400000000000000020000007473' +
  '000036ffffff0000020004ffffff00000107100000001c0000000400000000000000010000006400000008000c0004000800' +
  '080000000a0000000200000038ffffff00000104100000001400000004000000000000000300000062696e00b8ffffff5cff' +
  'ffff00000106100000001400000004000000000000000100000062000000dcffffff80ffffff000001051000000018000000' +
  '040000000000000001000000730000000400040004000000a8ffffff00000103100000001800000004000000000000000100' +
  '000066000600080006000600000000000200d4ffffff00000102100000001400000004000000000000000300000069333200' +
  'c4ffffff0000000120000000100014000800060007000c00000010001000000000000102100000001c000000040000000000' +
  '0000010000006900000008000c000800070008000000000000014000000000000000ffffffff880200001400000000000000' +
  '0c0016000600050008000c000c0000000003040018000000380100000000000000000a0018000c00040008000a0000009c01' +
  '0000100000000300000000000000000000001800000000000000000000000100000000000000080000000000000018000000' +
  '000000002000000000000000010000000000000028000000000000000c000000000000003800000000000000010000000000' +
  '0000400000000000000018000000000000005800000000000000010000000000000060000000000000001000000000000000' +
  '7000000000000000060000000000000078000000000000000100000000000000800000000000000001000000000000008800' +
  '000000000000010000000000000090000000000000001000000000000000a0000000000000000200000000000000a8000000' +
  '000000000100000000000000b0000000000000003000000000000000e0000000000000000100000000000000e80000000000' +
  '000018000000000000000001000000000000010000000000000008010000000000000c000000000000001801000000000000' +
  '0100000000000000200100000000000010000000000000003001000000000000000000000000000030010000000000000800' +
  '000000000000000000000b000000030000000000000001000000000000000300000000000000010000000000000003000000' +
  '0000000001000000000000000300000000000000010000000000000003000000000000000100000000000000030000000000' +
  '0000010000000000000003000000000000000100000000000000030000000000000001000000000000000300000000000000' +
  '0100000000000000030000000000000001000000000000000200000000000000000000000000000005000000000000000100' +
  '0000000000000000000000000000ffffffffffffdfff03000000000000000700000008000000000000000000000005000000' +
  '00000000000000000000f83f000000000000000000000000000002c005000000000000000000000006000000060000000600' +
  '000068c3a96c6c6f000003000000000000000100000000000000050000000000000000000000020000000200000002000000' +
  '00010000000000000500000000000000d204000000000000000000000000000000000000000000000000000000000000fbff' +
  'ffffffffffffffffffffffffffff0500000000000000b02b58c0ed0d06000000000000000000000000000000000005000000' +
  '00000000464d000000000000ffffffff000000000500000000000000000000000200000002000000020000000000803f0000' +
  '0040ffffffff00000000' +
  '',
  'hex',
);

const COLUMNS: ParamColumn[] = [
  { name: 'id', type: 'int64', values: [1, null, 9007199254740993n, -5] },
  { name: 'score', type: 'float64', values: [1.5, -0.25, null, 1e300] },
  { name: 'name', type: 'utf8', values: ['a', '', 'héllo wörld', null] },
  { name: 'flag', type: 'bool', values: [true, false, null, true] },
  { name: 'blob', type: 'binary', values: [Buffer.from([0, 1, 2]), null, Buffer.alloc(0), Buffer.from('xyz')] },
];

describe('arrow_ipc decoder', () => {
  it('decodes an independently produced stream (pyarrow) with every shipped type and nulls', () => {
    const { columns, rows } = decodeArrowStream(PYARROW_GOLDEN);
    assert.deepEqual(columns, ['i', 'i32', 'f', 's', 'b', 'bin', 'd', 'ts', 'dt', 'l']);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows.map((r) => r.i), [1, null, -9007199254740993n]);
    assert.deepEqual(rows.map((r) => r.i32), [7, 8, null]);
    assert.deepEqual(rows.map((r) => r.f), [1.5, null, -2.25]);
    assert.deepEqual(rows.map((r) => r.s), ['héllo', null, '']);
    assert.deepEqual(rows.map((r) => r.b), [true, false, null]);
    assert.deepEqual(rows.map((r) => r.bin), [Buffer.from([0, 1]), null, Buffer.alloc(0)]);
    assert.deepEqual(rows.map((r) => r.d), ['12.34', null, '-0.05']);
    assert.equal(rows[0].ts.toISOString(), '2024-01-02T03:04:05.678Z');
    assert.equal(rows[1].ts, null);
    assert.equal(rows[2].ts.getTime(), 0);
    assert.deepEqual(rows.map((r) => r.dt), ['2024-02-29', null, '1969-12-31']);
    assert.deepEqual(rows.map((r) => r.l), [[1, 2], null, []]);
  });
});

describe('arrow_ipc encoder', () => {
  const buf = encodeArrowStream(COLUMNS);

  it('frames Schema + RecordBatch + EOS with 8-byte alignment', () => {
    let off = 0;
    const messages: Array<{ metaLen: number; bodyStart: number }> = [];
    for (;;) {
      assert.equal(buf.readUInt32LE(off), 0xffffffff, `continuation marker at ${off}`);
      const metaLen = buf.readInt32LE(off + 4);
      if (metaLen === 0) {
        off += 8;
        break;
      }
      assert.equal((8 + metaLen) % 8, 0, 'metadata padded to 8 bytes');
      const bodyStart = off + 8 + metaLen;
      assert.equal(bodyStart % 8, 0, 'body starts 8-byte aligned');
      messages.push({ metaLen, bodyStart });
      // bodyLength is the 4th Message field; recover it by decoding the stream
      // prefix up to here instead of re-parsing the flatbuffer by hand.
      const meta = buf.subarray(off + 8, off + 8 + metaLen);
      const root = meta.readUInt32LE(0);
      const vt = root - meta.readInt32LE(root);
      const bodyLenOff = meta.readUInt16LE(vt + 4 + 2 * 3);
      const bodyLen = bodyLenOff ? Number(meta.readBigInt64LE(root + bodyLenOff)) : 0;
      assert.equal(bodyLen % 8, 0, 'body length is a multiple of 8');
      off = bodyStart + bodyLen;
    }
    assert.equal(messages.length, 2, 'one Schema and one RecordBatch message');
    assert.equal(off, buf.length, 'EOS is the last 8 bytes');
  });

  it('round-trips int64 / float64 / utf8 / bool / binary with nulls', () => {
    const { columns, rows } = decodeArrowStream(buf);
    assert.deepEqual(columns, COLUMNS.map((c) => c.name));
    assert.equal(rows.length, 4);
    for (const c of COLUMNS) {
      assert.deepEqual(rows.map((r) => r[c.name]), c.values, `column ${c.name}`);
    }
  });

  it('encodes an empty batch and a 10k-row batch', () => {
    assert.equal(decodeArrowStream(encodeArrowStream([{ name: 'x', type: 'int64', values: [] }])).rows.length, 0);
    const ids = Array.from({ length: 10_000 }, (_, i) => i + 1);
    const { rows } = decodeArrowStream(encodeArrowStream([{ name: 'id', type: 'int64', values: ids }]));
    assert.equal(rows.length, 10_000);
    assert.equal(rows.reduce((s, r) => s + r.id, 0), 50_005_000);
  });

  it('rejects values that do not match the column type', () => {
    assert.throws(() => encodeArrowStream([{ name: 'x', type: 'int64', values: [1.5] }]), TypeError);
    assert.throws(() => encodeArrowStream([{ name: 'x', type: 'int64', values: [2 ** 60] }]), RangeError);
    assert.throws(() => encodeArrowStream([{ name: 'x', type: 'utf8', values: [1] }]), TypeError);
    assert.throws(
      () => encodeArrowStream([{ name: 'a', type: 'int64', values: [1] }, { name: 'b', type: 'int64', values: [] }]),
      RangeError,
    );
  });

  it('infers parameter types and normalizes pg-style values', () => {
    assert.equal(inferParamType([1, null, 2n], 'c'), 'int64');
    assert.equal(inferParamType([1, 2.5], 'c'), 'float64');
    assert.equal(inferParamType(['a', null], 'c'), 'utf8');
    assert.equal(inferParamType([true], 'c'), 'bool');
    assert.equal(inferParamType([Buffer.from('x')], 'c'), 'binary');
    assert.equal(inferParamType([null, undefined], 'c'), 'utf8');
    assert.throws(() => inferParamType([1, 'a'], 'c'), TypeError);
    assert.equal(normalizeParam(new Date(0)), '1970-01-01T00:00:00.000Z');
    assert.equal(normalizeParam({ a: 1 }), '{"a":1}');
    assert.equal(normalizeParam([1, 'x"y', null]), '{"1","x\\"y",NULL}');
  });

  const py = spawnSync('python3', ['-I', '-c', 'import pyarrow'], { encoding: 'utf-8' });
  const havePyarrow = py.status === 0;

  it('is readable by pyarrow (independent reader)', { skip: havePyarrow ? false : 'python3 + pyarrow not available' }, () => {
    const script =
      'import sys, json, pyarrow as pa\n' +
      'r = pa.ipc.open_stream(sys.stdin.buffer.read())\n' +
      't = r.read_all()\n' +
      'print(json.dumps({"types": [str(f.type) for f in t.schema], "rows": [[None if v is None else (v.hex() if isinstance(v, bytes) else v) for v in row.values()] for row in t.to_pylist()]}))\n';
    const out = spawnSync('python3', ['-I', '-c', script], { input: buf, encoding: 'utf-8' });
    assert.equal(out.status, 0, out.stderr);
    const parsed = JSON.parse(out.stdout);
    assert.deepEqual(parsed.types, ['int64', 'double', 'string', 'bool', 'binary']);
    assert.deepEqual(parsed.rows, [
      [1, 1.5, 'a', true, '000102'],
      [null, -0.25, '', false, null],
      [9007199254740993, null, 'héllo wörld', null, ''],
      [-5, 1e300, null, true, '78797a'],
    ]);
  });
});
