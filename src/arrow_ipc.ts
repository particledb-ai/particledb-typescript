/**
 * Dependency-free Arrow IPC stream codec used by the PDB native wire client.
 *
 * - {@link decodeArrowStream} reads the Arrow IPC streams the server sends in
 *   `MSG_ARROW_RESULT` / `MSG_ARROW_STREAM` / BATCH_RESULT entries and turns
 *   them into plain row objects.
 * - {@link encodeArrowStream} writes the Arrow IPC stream (one Schema message,
 *   one RecordBatch message, end-of-stream marker) that `MSG_EXECUTE` and
 *   `MSG_EXECUTE_BATCH` carry as their parameter payload.
 *
 * Both sides use the encapsulated message format: `0xFFFFFFFF` continuation
 * marker, `i32` metadata length (padded so the body starts 8-byte aligned),
 * the `Message` flatbuffer, then the body with every buffer 8-byte aligned.
 * The flatbuffers are read and written by hand (Schema.fbs / Message.fbs,
 * MetadataVersion V5, little-endian), so the package needs no Arrow library.
 */

/** A decoded row keyed by column name. */
export type ArrowRow = Record<string, any>;

/** Parameter column types the writer can encode. */
export type ParamType = 'int64' | 'float64' | 'utf8' | 'bool' | 'binary';

/** Thrown for a malformed stream or an Arrow type this codec does not handle. */
export class ArrowCodecError extends Error {
  /** True when the stream is well-formed but uses an unsupported type. */
  readonly unsupported: boolean;
  constructor(message: string, unsupported = false) {
    super(message);
    this.name = 'ArrowCodecError';
    this.unsupported = unsupported;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

// Type union tags (Schema.fbs `union Type`).
const T_NULL = 1;
const T_INT = 2;
const T_FLOAT = 3;
const T_BINARY = 4;
const T_UTF8 = 5;
const T_BOOL = 6;
const T_DECIMAL = 7;
const T_DATE = 8;
const T_TIME = 9;
const T_TIMESTAMP = 10;
const T_INTERVAL = 11;
const T_LIST = 12;
const T_STRUCT = 13;
const T_FIXED_SIZE_BINARY = 15;
const T_FIXED_SIZE_LIST = 16;
const T_DURATION = 18;
const T_LARGE_BINARY = 19;
const T_LARGE_UTF8 = 20;
const T_LARGE_LIST = 21;

// MessageHeader union tags.
const H_SCHEMA = 1;
const H_DICTIONARY_BATCH = 2;
const H_RECORD_BATCH = 3;

const METADATA_V5 = 4;
const CONTINUATION = 0xffffffff;

const pad8 = (n: number): number => (n + 7) & ~7;

// ===========================================================================
// Flatbuffer reading
// ===========================================================================

class FbTable {
  private readonly vt: number;
  private readonly vtSize: number;
  constructor(readonly buf: Buffer, readonly pos: number) {
    this.vt = pos - buf.readInt32LE(pos);
    this.vtSize = buf.readUInt16LE(this.vt);
  }
  private slot(i: number): number {
    const o = 4 + 2 * i;
    return o < this.vtSize ? this.buf.readUInt16LE(this.vt + o) : 0;
  }
  u8(i: number, d = 0): number { const o = this.slot(i); return o ? this.buf.readUInt8(this.pos + o) : d; }
  i16(i: number, d = 0): number { const o = this.slot(i); return o ? this.buf.readInt16LE(this.pos + o) : d; }
  i32(i: number, d = 0): number { const o = this.slot(i); return o ? this.buf.readInt32LE(this.pos + o) : d; }
  i64(i: number, d = 0): number {
    const o = this.slot(i);
    return o ? Number(this.buf.readBigInt64LE(this.pos + o)) : d;
  }
  bool(i: number, d = false): boolean { const o = this.slot(i); return o ? this.buf[this.pos + o] !== 0 : d; }
  /** Absolute position of the object an offset field points at, or 0. */
  ref(i: number): number {
    const o = this.slot(i);
    if (!o) return 0;
    const p = this.pos + o;
    return p + this.buf.readUInt32LE(p);
  }
  table(i: number): FbTable | null {
    const p = this.ref(i);
    return p ? new FbTable(this.buf, p) : null;
  }
  str(i: number): string | null {
    const p = this.ref(i);
    if (!p) return null;
    const len = this.buf.readUInt32LE(p);
    return this.buf.toString('utf-8', p + 4, p + 4 + len);
  }
  /** [start, length] of a vector field, or null. */
  vec(i: number): [number, number] | null {
    const p = this.ref(i);
    return p ? [p + 4, this.buf.readUInt32LE(p)] : null;
  }
  tables(i: number): FbTable[] {
    const v = this.vec(i);
    if (!v) return [];
    const out: FbTable[] = [];
    for (let k = 0; k < v[1]; k++) {
      const p = v[0] + 4 * k;
      out.push(new FbTable(this.buf, p + this.buf.readUInt32LE(p)));
    }
    return out;
  }
}

function rootTable(buf: Buffer): FbTable {
  return new FbTable(buf, buf.readUInt32LE(0));
}

// ===========================================================================
// Decoder
// ===========================================================================

interface FieldInfo {
  name: string;
  nullable: boolean;
  typeTag: number;
  type: FbTable | null;
  children: FieldInfo[];
  metadata: Record<string, string>;
}

/**
 * One result column as the server described it in the Arrow schema: its
 * name, the Arrow type as a short token, and the field metadata the engine
 * stamps (notably `pdb:decl`, the column's DECLARED SQL type such as
 * `uuid`, `jsonb`, `numeric(10,2)`, `timestamptz` or `int[]`).
 */
export interface ResultField {
  name: string;
  /**
   * Arrow type token: `null`, `int8`..`int64`, `uint8`..`uint64`, `float32`,
   * `float64`, `bool`, `utf8`, `binary`, `fixed_size_binary`, `decimal`,
   * `date`, `timestamp`, `time`, `duration`, `interval`, `list`,
   * `fixed_size_list`, `struct`, or `unknown`.
   */
  type: string;
  nullable: boolean;
  /** The engine's `pdb:decl` stamp (declared SQL type), when present. */
  decl?: string;
  /** Timestamp columns: the Arrow timezone (an instant when set). */
  timezone?: string;
  /** Element field of a list column. */
  element?: ResultField;
  /** Every key/value of the field's custom metadata. */
  metadata: Record<string, string>;
}

function typeToken(f: FieldInfo): string {
  switch (f.typeTag) {
    case T_NULL: return 'null';
    case T_INT: return `${f.type!.bool(1) ? 'int' : 'uint'}${f.type!.i32(0)}`;
    case T_FLOAT: return f.type!.i16(0) === 1 ? 'float32' : f.type!.i16(0) === 2 ? 'float64' : 'float16';
    case T_BOOL: return 'bool';
    case T_UTF8: case T_LARGE_UTF8: return 'utf8';
    case T_BINARY: case T_LARGE_BINARY: return 'binary';
    case T_FIXED_SIZE_BINARY: return 'fixed_size_binary';
    case T_DECIMAL: return 'decimal';
    case T_DATE: return 'date';
    case T_TIMESTAMP: return 'timestamp';
    case T_TIME: return 'time';
    case T_DURATION: return 'duration';
    case T_INTERVAL: return 'interval';
    case T_LIST: case T_LARGE_LIST: return 'list';
    case T_FIXED_SIZE_LIST: return 'fixed_size_list';
    case T_STRUCT: return 'struct';
    default: return 'unknown';
  }
}

function toResultField(f: FieldInfo): ResultField {
  const out: ResultField = { name: f.name, type: typeToken(f), nullable: f.nullable, metadata: f.metadata };
  if (f.metadata['pdb:decl'] !== undefined) out.decl = f.metadata['pdb:decl'];
  if (f.typeTag === T_TIMESTAMP) {
    const tz = f.type!.str(1);
    if (tz) out.timezone = tz;
  }
  if ((f.typeTag === T_LIST || f.typeTag === T_LARGE_LIST || f.typeTag === T_FIXED_SIZE_LIST) && f.children[0]) {
    out.element = toResultField(f.children[0]);
  }
  return out;
}

function readField(t: FbTable): FieldInfo {
  if (t.table(4)) {
    throw new ArrowCodecError('dictionary-encoded result columns are not supported', true);
  }
  return {
    name: t.str(0) ?? '',
    nullable: t.bool(1),
    typeTag: t.u8(2),
    type: t.table(3),
    children: t.tables(5).map(readField),
    metadata: readMetadata(t),
  };
}

/** Field.custom_metadata: a vector of KeyValue { key: string, value: string }. */
function readMetadata(t: FbTable): Record<string, string> {
  const out: Record<string, string> = {};
  for (const kv of t.tables(6)) {
    const k = kv.str(0);
    if (k !== null) out[k] = kv.str(1) ?? '';
  }
  return out;
}

/** Sequential cursor over a RecordBatch's field nodes and buffers. */
class BatchCursor {
  private node = 0;
  private bufIdx = 0;
  constructor(
    private readonly body: Buffer,
    private readonly nodes: Array<[number, number]>,
    private readonly buffers: Array<[number, number]>,
  ) {}
  nextNode(): [number, number] {
    if (this.node >= this.nodes.length) throw new ArrowCodecError('record batch: missing field node');
    return this.nodes[this.node++];
  }
  nextBuffer(): Buffer {
    if (this.bufIdx >= this.buffers.length) throw new ArrowCodecError('record batch: missing buffer');
    const [off, len] = this.buffers[this.bufIdx++];
    if (off + len > this.body.length) throw new ArrowCodecError('record batch: buffer out of bounds');
    return this.body.subarray(off, off + len);
  }
}

const bitSet = (b: Buffer, i: number): boolean => (b[i >> 3] & (1 << (i & 7))) !== 0;

function int64ToJs(v: bigint): number | bigint {
  return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
}

function decimalToString(raw: bigint, scale: number): string {
  const neg = raw < 0n;
  let s = (neg ? -raw : raw).toString();
  if (scale > 0) {
    s = s.padStart(scale + 1, '0');
    s = s.slice(0, s.length - scale) + '.' + s.slice(s.length - scale);
  } else if (scale < 0) {
    s = s + '0'.repeat(-scale);
  }
  return neg ? '-' + s : s;
}

function readI128(b: Buffer, off: number): bigint {
  const lo = b.readBigUInt64LE(off);
  const hi = b.readBigInt64LE(off + 8);
  return (hi << 64n) | lo;
}

/** Little-endian two's-complement 256-bit integer (Decimal256 storage). */
function readI256(b: Buffer, off: number): bigint {
  let v = 0n;
  for (let w = 3; w >= 0; w--) v = (v << 64n) | b.readBigUInt64LE(off + 8 * w);
  return v >= 1n << 255n ? v - (1n << 256n) : v;
}

const UNIT_DIV: Record<number, bigint> = { 0: 1n, 1: 1n, 2: 1000n, 3: 1000000n };
const UNIT_MUL: Record<number, bigint> = { 0: 1000n, 1: 1n, 2: 1n, 3: 1n };

/** Options for {@link decodeArrowStream}. */
export interface DecodeOptions {
  /**
   * How Timestamp columns decode: `'date'` (default) as a JS `Date`
   * (millisecond precision), or `'string'` as the exact UTC wall-clock text
   * `YYYY-MM-DD HH:MM:SS[.ffffff]` (fraction's trailing zeros trimmed), which
   * keeps microseconds.
   */
  timestamps?: 'date' | 'string';
}

/** Units per second for Arrow TimeUnit SECOND / MILLI / MICRO / NANO. */
const UNITS_PER_SEC: Record<number, bigint> = { 0: 1n, 1: 1000n, 2: 1000000n, 3: 1000000000n };

/** Exact `YYYY-MM-DD HH:MM:SS[.f]` (UTC) of a raw Arrow timestamp. */
function timestampText(raw: bigint, unit: number): string {
  const per = UNITS_PER_SEC[unit] ?? 1000000n;
  let secs = raw / per;
  let frac = raw % per;
  if (frac < 0n) { frac += per; secs -= 1n; }
  const d = new Date(Number(secs) * 1000);
  const y = d.getUTCFullYear();
  const pad = (n: number, w = 2) => String(n).padStart(w, '0');
  const year = y < 0 ? '-' + pad(-y, 4) : pad(y, 4);
  let out = `${year}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
  if (frac !== 0n) {
    const digits = per.toString().length - 1;
    out += '.' + frac.toString().padStart(digits, '0').replace(/0+$/, '');
  }
  return out;
}

/** Decode one column (recursively for nested types) into an array of JS values. */
function readColumn(f: FieldInfo, cur: BatchCursor, opts: DecodeOptions = {}): any[] {
  const [length, nullCount] = cur.nextNode();
  const out: any[] = new Array(length);
  if (f.typeTag === T_NULL) {
    out.fill(null);
    return out;
  }
  const validityBuf = cur.nextBuffer();
  const valid = (i: number): boolean => nullCount === 0 || validityBuf.length === 0 || bitSet(validityBuf, i);

  switch (f.typeTag) {
    case T_INT: {
      const bits = f.type!.i32(0);
      const signed = f.type!.bool(1);
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        switch (bits) {
          case 8: out[i] = signed ? data.readInt8(i) : data.readUInt8(i); break;
          case 16: out[i] = signed ? data.readInt16LE(2 * i) : data.readUInt16LE(2 * i); break;
          case 32: out[i] = signed ? data.readInt32LE(4 * i) : data.readUInt32LE(4 * i); break;
          case 64: out[i] = int64ToJs(signed ? data.readBigInt64LE(8 * i) : data.readBigUInt64LE(8 * i)); break;
          default: throw new ArrowCodecError(`unsupported Int bit width ${bits}`, true);
        }
      }
      return out;
    }
    case T_FLOAT: {
      const precision = f.type!.i16(0);
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        if (precision === 2) out[i] = data.readDoubleLE(8 * i);
        else if (precision === 1) out[i] = data.readFloatLE(4 * i);
        else throw new ArrowCodecError('unsupported half-precision float column', true);
      }
      return out;
    }
    case T_BOOL: {
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) out[i] = valid(i) ? bitSet(data, i) : null;
      return out;
    }
    case T_UTF8:
    case T_BINARY:
    case T_LARGE_UTF8:
    case T_LARGE_BINARY: {
      const large = f.typeTag === T_LARGE_UTF8 || f.typeTag === T_LARGE_BINARY;
      const text = f.typeTag === T_UTF8 || f.typeTag === T_LARGE_UTF8;
      const offsets = cur.nextBuffer();
      const data = cur.nextBuffer();
      const at = (i: number): number => (large ? Number(offsets.readBigInt64LE(8 * i)) : offsets.readInt32LE(4 * i));
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        const s = at(i);
        const e = at(i + 1);
        out[i] = text ? data.toString('utf-8', s, e) : Buffer.from(data.subarray(s, e));
      }
      return out;
    }
    case T_FIXED_SIZE_BINARY: {
      const w = f.type!.i32(0);
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) out[i] = valid(i) ? Buffer.from(data.subarray(i * w, (i + 1) * w)) : null;
      return out;
    }
    case T_DECIMAL: {
      const scale = f.type!.i32(1);
      const bitWidth = f.type!.i32(2, 128);
      if (bitWidth !== 128 && bitWidth !== 256) throw new ArrowCodecError(`unsupported Decimal bit width ${bitWidth}`, true);
      const data = cur.nextBuffer();
      const read = bitWidth === 128 ? (k: number) => readI128(data, 16 * k) : (k: number) => readI256(data, 32 * k);
      for (let i = 0; i < length; i++) out[i] = valid(i) ? decimalToString(read(i), scale) : null;
      return out;
    }
    case T_DATE: {
      const unit = f.type!.i16(0, 1); // DAY=0, MILLISECOND=1
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        const ms = unit === 0 ? data.readInt32LE(4 * i) * 86_400_000 : Number(data.readBigInt64LE(8 * i));
        out[i] = new Date(ms).toISOString().slice(0, 10);
      }
      return out;
    }
    case T_TIMESTAMP: {
      const unit = f.type!.i16(0);
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        const raw = data.readBigInt64LE(8 * i);
        out[i] = opts.timestamps === 'string'
          ? timestampText(raw, unit)
          : new Date(Number((raw * UNIT_MUL[unit]) / UNIT_DIV[unit]));
      }
      return out;
    }
    case T_TIME:
    case T_DURATION: {
      // Raw integer in the column's declared unit.
      const bits = f.typeTag === T_TIME ? f.type!.i32(1, 32) : 64;
      const data = cur.nextBuffer();
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        out[i] = bits === 32 ? data.readInt32LE(4 * i) : int64ToJs(data.readBigInt64LE(8 * i));
      }
      return out;
    }
    case T_LIST:
    case T_LARGE_LIST: {
      const large = f.typeTag === T_LARGE_LIST;
      const offsets = cur.nextBuffer();
      const child = readColumn(f.children[0], cur, opts);
      const at = (i: number): number => (large ? Number(offsets.readBigInt64LE(8 * i)) : offsets.readInt32LE(4 * i));
      for (let i = 0; i < length; i++) out[i] = valid(i) ? child.slice(at(i), at(i + 1)) : null;
      return out;
    }
    case T_FIXED_SIZE_LIST: {
      const n = f.type!.i32(0);
      const child = readColumn(f.children[0], cur, opts);
      for (let i = 0; i < length; i++) out[i] = valid(i) ? child.slice(i * n, (i + 1) * n) : null;
      return out;
    }
    case T_STRUCT: {
      const cols = f.children.map((c) => [c.name, readColumn(c, cur, opts)] as [string, any[]]);
      for (let i = 0; i < length; i++) {
        if (!valid(i)) { out[i] = null; continue; }
        const o: ArrowRow = {};
        for (const [name, vals] of cols) o[name] = vals[i];
        out[i] = o;
      }
      return out;
    }
    case T_INTERVAL:
    default:
      throw new ArrowCodecError(`unsupported Arrow result type (type tag ${f.typeTag})`, true);
  }
}

/** Result of {@link decodeArrowStream}. */
export interface DecodedStream<R = ArrowRow> {
  columns: string[];
  rows: R[];
  /** The schema's fields, in column order (empty when the stream had none). */
  fields: ResultField[];
}

/**
 * Decode an Arrow IPC stream (Schema message, then RecordBatch messages,
 * optional end-of-stream marker) into column names and row objects -- or,
 * with `shape: 'array'`, into positional row arrays, which keep two columns
 * with the same label apart (`SELECT a.id, b.id ...`).
 */
export function decodeArrowStream(buf: Buffer): DecodedStream<ArrowRow>;
export function decodeArrowStream(buf: Buffer, shape: 'object', opts?: DecodeOptions): DecodedStream<ArrowRow>;
export function decodeArrowStream(buf: Buffer, shape: 'array', opts?: DecodeOptions): DecodedStream<any[]>;
export function decodeArrowStream(
  buf: Buffer,
  shape: 'object' | 'array' = 'object',
  opts: DecodeOptions = {},
): DecodedStream<any> {
  let off = 0;
  let fields: FieldInfo[] | null = null;
  const rows: any[] = [];
  while (off + 4 <= buf.length) {
    let metaLen = buf.readInt32LE(off);
    off += 4;
    if (metaLen === -1) {
      if (off + 4 > buf.length) break;
      metaLen = buf.readInt32LE(off);
      off += 4;
    }
    if (metaLen === 0) break; // end of stream
    if (metaLen < 0 || off + metaLen > buf.length) {
      throw new ArrowCodecError('Arrow IPC: metadata length out of bounds');
    }
    const meta = buf.subarray(off, off + metaLen);
    off += metaLen;
    const msg = rootTable(meta);
    const headerType = msg.u8(1);
    const header = msg.table(2);
    const bodyLen = msg.i64(3);
    if (bodyLen < 0 || off + bodyLen > buf.length) {
      throw new ArrowCodecError('Arrow IPC: body length out of bounds');
    }
    const body = buf.subarray(off, off + bodyLen);
    off += bodyLen;
    if (!header) throw new ArrowCodecError('Arrow IPC: message without header');

    if (headerType === H_SCHEMA) {
      fields = header.tables(1).map(readField);
    } else if (headerType === H_RECORD_BATCH) {
      if (!fields) throw new ArrowCodecError('Arrow IPC: record batch before schema');
      if (header.table(3)) throw new ArrowCodecError('compressed Arrow record batches are not supported', true);
      const length = header.i64(0);
      const nodes: Array<[number, number]> = [];
      const nv = header.vec(1);
      if (nv) for (let k = 0; k < nv[1]; k++) {
        nodes.push([Number(meta.readBigInt64LE(nv[0] + 16 * k)), Number(meta.readBigInt64LE(nv[0] + 16 * k + 8))]);
      }
      const buffers: Array<[number, number]> = [];
      const bv = header.vec(2);
      if (bv) for (let k = 0; k < bv[1]; k++) {
        buffers.push([Number(meta.readBigInt64LE(bv[0] + 16 * k)), Number(meta.readBigInt64LE(bv[0] + 16 * k + 8))]);
      }
      const cur = new BatchCursor(body, nodes, buffers);
      const cols = fields.map((f) => readColumn(f, cur, opts));
      if (shape === 'array') {
        for (let i = 0; i < length; i++) {
          const r = new Array(fields.length);
          for (let c = 0; c < fields.length; c++) r[c] = cols[c][i];
          rows.push(r);
        }
        continue;
      }
      for (let i = 0; i < length; i++) {
        const r: ArrowRow = {};
        for (let c = 0; c < fields.length; c++) r[fields[c].name] = cols[c][i];
        rows.push(r);
      }
    } else if (headerType === H_DICTIONARY_BATCH) {
      throw new ArrowCodecError('Arrow dictionary batches are not supported', true);
    } else {
      throw new ArrowCodecError(`Arrow IPC: unexpected message header type ${headerType}`, true);
    }
  }
  return {
    columns: fields ? fields.map((f) => f.name) : [],
    rows,
    fields: fields ? fields.map(toResultField) : [],
  };
}

// ===========================================================================
// Flatbuffer writing (back-to-front builder, standard flatbuffers layout)
// ===========================================================================

class FbBuilder {
  private buf: Buffer;
  private space: number;
  private minAlign = 1;
  private vtable: number[] | null = null;
  private objectEnd = 0;

  constructor(initial = 1024) {
    this.buf = Buffer.alloc(initial);
    this.space = initial;
  }

  offset(): number {
    return this.buf.length - this.space;
  }

  private grow(): void {
    const old = this.buf;
    const next = Buffer.alloc(old.length * 2);
    old.copy(next, old.length);
    this.space += old.length;
    this.buf = next;
  }

  private pad(n: number): void {
    for (let i = 0; i < n; i++) this.buf[--this.space] = 0;
  }

  /** Align so that after writing `additional` bytes, a `size`-byte value is aligned. */
  prep(size: number, additional: number): void {
    if (size > this.minAlign) this.minAlign = size;
    const alignSize = (~(this.buf.length - this.space + additional) + 1) & (size - 1);
    while (this.space < alignSize + size + additional) this.grow();
    this.pad(alignSize);
  }

  addU8(v: number): void { this.prep(1, 0); this.buf.writeUInt8(v & 0xff, --this.space); }
  addI16(v: number): void { this.prep(2, 0); this.space -= 2; this.buf.writeInt16LE(v, this.space); }
  addI32(v: number): void { this.prep(4, 0); this.space -= 4; this.buf.writeInt32LE(v, this.space); }
  addI64(v: bigint): void { this.prep(8, 0); this.space -= 8; this.buf.writeBigInt64LE(v, this.space); }
  addOffset(off: number): void {
    this.prep(4, 0);
    const rel = this.offset() - off + 4;
    this.space -= 4;
    this.buf.writeUInt32LE(rel, this.space);
  }

  startTable(numFields: number): void {
    this.vtable = new Array(numFields).fill(0);
    this.objectEnd = this.offset();
  }
  private slot(i: number): void { this.vtable![i] = this.offset(); }
  fieldU8(i: number, v: number): void { this.addU8(v); this.slot(i); }
  fieldI16(i: number, v: number): void { this.addI16(v); this.slot(i); }
  fieldI32(i: number, v: number): void { this.addI32(v); this.slot(i); }
  fieldI64(i: number, v: bigint): void { this.addI64(v); this.slot(i); }
  fieldOffset(i: number, off: number): void { this.addOffset(off); this.slot(i); }

  endTable(): number {
    const vt = this.vtable!;
    this.addI32(0); // soffset placeholder
    const objectOffset = this.offset();
    for (let i = vt.length - 1; i >= 0; i--) this.addI16(vt[i] ? objectOffset - vt[i] : 0);
    this.addI16(objectOffset - this.objectEnd);
    this.addI16((vt.length + 2) * 2);
    const vtOffset = this.offset();
    this.buf.writeInt32LE(vtOffset - objectOffset, this.buf.length - objectOffset);
    this.vtable = null;
    return objectOffset;
  }

  createString(s: string): number {
    const bytes = Buffer.from(s, 'utf-8');
    this.prep(4, bytes.length + 1);
    this.buf[--this.space] = 0;
    this.space -= bytes.length;
    bytes.copy(this.buf, this.space);
    this.space -= 4;
    this.buf.writeUInt32LE(bytes.length, this.space);
    return this.offset();
  }

  createOffsetVector(offs: number[]): number {
    this.prep(4, 4 * offs.length);
    for (let i = offs.length - 1; i >= 0; i--) this.addOffset(offs[i]);
    this.space -= 4;
    this.buf.writeUInt32LE(offs.length, this.space);
    return this.offset();
  }

  /** Vector of 16-byte structs made of two int64 fields each. */
  createPairVector(pairs: Array<[number, number]>): number {
    this.prep(4, 16 * pairs.length);
    this.prep(8, 16 * pairs.length);
    for (let i = pairs.length - 1; i >= 0; i--) {
      this.space -= 8;
      this.buf.writeBigInt64LE(BigInt(pairs[i][1]), this.space);
      this.space -= 8;
      this.buf.writeBigInt64LE(BigInt(pairs[i][0]), this.space);
    }
    this.space -= 4;
    this.buf.writeUInt32LE(pairs.length, this.space);
    return this.offset();
  }

  finish(root: number): Buffer {
    this.prep(this.minAlign, 4);
    this.addOffset(root);
    return Buffer.from(this.buf.subarray(this.space));
  }
}

// ===========================================================================
// Encoder
// ===========================================================================

/** One parameter column: a name, a type, and one value per row. */
export interface ParamColumn {
  name: string;
  type: ParamType;
  values: any[];
}

function writeTypeTable(b: FbBuilder, type: ParamType): [number, number] {
  switch (type) {
    case 'int64':
      b.startTable(2);
      b.fieldI32(0, 64);
      b.fieldU8(1, 1);
      return [T_INT, b.endTable()];
    case 'float64':
      b.startTable(1);
      b.fieldI16(0, 2); // DOUBLE
      return [T_FLOAT, b.endTable()];
    case 'utf8':
      b.startTable(0);
      return [T_UTF8, b.endTable()];
    case 'bool':
      b.startTable(0);
      return [T_BOOL, b.endTable()];
    case 'binary':
      b.startTable(0);
      return [T_BINARY, b.endTable()];
    default:
      throw new ArrowCodecError(`unsupported parameter type '${String(type)}'`, true);
  }
}

function messageBytes(build: (b: FbBuilder) => [number, number], bodyLength: number): Buffer {
  const b = new FbBuilder();
  const [headerType, header] = build(b);
  b.startTable(5);
  b.fieldI64(3, BigInt(bodyLength));
  b.fieldOffset(2, header);
  b.fieldI16(0, METADATA_V5);
  b.fieldU8(1, headerType);
  return b.finish(b.endTable());
}

/** Frame one encapsulated message: marker, padded metadata length, metadata, body. */
function frameMessage(meta: Buffer, body: Buffer[]): Buffer[] {
  const metaPadded = pad8(8 + meta.length) - 8;
  const prefix = Buffer.alloc(8);
  prefix.writeUInt32LE(CONTINUATION, 0);
  prefix.writeInt32LE(metaPadded, 4);
  const parts = [prefix, meta];
  if (metaPadded > meta.length) parts.push(Buffer.alloc(metaPadded - meta.length));
  return parts.concat(body);
}

/** 2^53: every integer of at most this magnitude is exact in a double. */
const MAX_EXACT_FLOAT_INT = 1n << 53n;

function toBigInt64(v: any, col: string): bigint {
  if (typeof v === 'bigint') {
    if (v < -(1n << 63n) || v >= 1n << 63n) throw new RangeError(`column ${col}: ${v} does not fit int64`);
    return v;
  }
  if (typeof v === 'number' && Number.isInteger(v)) {
    if (!Number.isSafeInteger(v)) throw new RangeError(`column ${col}: ${v} is not a safe integer; pass a bigint`);
    return BigInt(v);
  }
  if (typeof v === 'string' && /^-?\d+$/.test(v)) return BigInt(v);
  throw new TypeError(`column ${col}: ${String(v)} is not an int64 value`);
}

function columnBuffers(col: ParamColumn): { nullCount: number; buffers: Buffer[] } {
  const n = col.values.length;
  const validity = Buffer.alloc(pad8((n + 7) >> 3));
  let nullCount = 0;
  for (let i = 0; i < n; i++) {
    const v = col.values[i];
    if (v === null || v === undefined) nullCount++;
    else validity[i >> 3] |= 1 << (i & 7);
  }
  const isNull = (i: number): boolean => col.values[i] === null || col.values[i] === undefined;
  switch (col.type) {
    case 'int64': {
      const data = Buffer.alloc(8 * n);
      for (let i = 0; i < n; i++) if (!isNull(i)) data.writeBigInt64LE(toBigInt64(col.values[i], col.name), 8 * i);
      return { nullCount, buffers: [validity, data] };
    }
    case 'float64': {
      const data = Buffer.alloc(8 * n);
      for (let i = 0; i < n; i++) {
        if (isNull(i)) continue;
        const v = col.values[i];
        if (typeof v === 'bigint' && (v > MAX_EXACT_FLOAT_INT || v < -MAX_EXACT_FLOAT_INT)) {
          // Number(v) would bind a different value than the caller passed.
          throw new RangeError(
            `column ${col.name}: bigint ${v} in a float64 column is not exact as a double (|v| > 2^53); ` +
              'keep the column integer or pass explicit types',
          );
        }
        const num = typeof v === 'bigint' ? Number(v) : v;
        if (typeof num !== 'number') throw new TypeError(`column ${col.name}: ${String(v)} is not a number`);
        data.writeDoubleLE(num, 8 * i);
      }
      return { nullCount, buffers: [validity, data] };
    }
    case 'bool': {
      const data = Buffer.alloc(pad8((n + 7) >> 3));
      for (let i = 0; i < n; i++) {
        if (isNull(i)) continue;
        const v = col.values[i];
        if (typeof v !== 'boolean') throw new TypeError(`column ${col.name}: ${String(v)} is not a boolean`);
        if (v) data[i >> 3] |= 1 << (i & 7);
      }
      return { nullCount, buffers: [validity, data] };
    }
    case 'utf8':
    case 'binary': {
      const parts: Buffer[] = [];
      const offsets = Buffer.alloc(4 * (n + 1));
      let total = 0;
      for (let i = 0; i < n; i++) {
        if (!isNull(i)) {
          const v = col.values[i];
          let bytes: Buffer;
          if (col.type === 'utf8') {
            if (typeof v !== 'string') throw new TypeError(`column ${col.name}: ${String(v)} is not a string`);
            bytes = Buffer.from(v, 'utf-8');
          } else {
            if (!(v instanceof Uint8Array)) throw new TypeError(`column ${col.name}: binary values must be Buffer/Uint8Array`);
            bytes = Buffer.from(v.buffer, v.byteOffset, v.byteLength);
          }
          parts.push(bytes);
          total += bytes.length;
          if (total > 0x7fffffff) throw new RangeError(`column ${col.name}: more than 2 GiB of data`);
        }
        offsets.writeInt32LE(total, 4 * (i + 1));
      }
      return { nullCount, buffers: [validity, offsets, Buffer.concat(parts, total)] };
    }
    default:
      throw new ArrowCodecError(`unsupported parameter type '${String(col.type)}'`, true);
  }
}

/**
 * Encode `columns` (all the same length) as an Arrow IPC stream: one Schema
 * message, one RecordBatch message, and the end-of-stream marker.
 */
export function encodeArrowStream(columns: ParamColumn[]): Buffer {
  const nrows = columns.length ? columns[0].values.length : 0;
  for (const c of columns) {
    if (c.values.length !== nrows) throw new RangeError(`column ${c.name}: ${c.values.length} values, expected ${nrows}`);
  }

  // Schema message.
  const schemaMeta = messageBytes((b) => {
    const fieldOffs = columns.map((c) => {
      const name = b.createString(c.name);
      const [tag, typeOff] = writeTypeTable(b, c.type);
      const children = b.createOffsetVector([]);
      b.startTable(7);
      b.fieldOffset(0, name);
      b.fieldOffset(3, typeOff);
      b.fieldOffset(5, children);
      b.fieldU8(1, 1); // nullable
      b.fieldU8(2, tag);
      return b.endTable();
    });
    const fieldsVec = b.createOffsetVector(fieldOffs);
    b.startTable(4);
    b.fieldOffset(1, fieldsVec);
    b.fieldI16(0, 0); // Little endian
    return [H_SCHEMA, b.endTable()];
  }, 0);

  // RecordBatch body: every buffer 8-byte aligned.
  const nodes: Array<[number, number]> = [];
  const bufMeta: Array<[number, number]> = [];
  const body: Buffer[] = [];
  let bodyLen = 0;
  for (const c of columns) {
    const { nullCount, buffers } = columnBuffers(c);
    nodes.push([nrows, nullCount]);
    for (const buf of buffers) {
      bufMeta.push([bodyLen, buf.length]);
      body.push(buf);
      bodyLen += buf.length;
      const padded = pad8(buf.length);
      if (padded > buf.length) {
        body.push(Buffer.alloc(padded - buf.length));
        bodyLen += padded - buf.length;
      }
    }
  }
  const batchMeta = messageBytes((b) => {
    const buffersVec = b.createPairVector(bufMeta);
    const nodesVec = b.createPairVector(nodes);
    b.startTable(5);
    b.fieldI64(0, BigInt(nrows));
    b.fieldOffset(1, nodesVec);
    b.fieldOffset(2, buffersVec);
    return [H_RECORD_BATCH, b.endTable()];
  }, bodyLen);

  const eos = Buffer.alloc(8);
  eos.writeUInt32LE(CONTINUATION, 0);
  return Buffer.concat([...frameMessage(schemaMeta, []), ...frameMessage(batchMeta, body), eos]);
}

/**
 * Infer a parameter type from a column of JS values: integers / bigint →
 * int64 (float64 if any value is fractional), string → utf8, boolean → bool,
 * Buffer / Uint8Array → binary. An all-null column is sent as utf8 nulls.
 * Any other value (Date, object, array) is converted by {@link normalizeParam}
 * before inference. Mixed incompatible kinds throw `TypeError`.
 */
export function inferParamType(values: any[], col: string): ParamType {
  let kind: ParamType | null = null;
  for (const v of values) {
    if (v === null || v === undefined) continue;
    let k: ParamType;
    if (typeof v === 'bigint') k = 'int64';
    else if (typeof v === 'number') k = Number.isSafeInteger(v) ? 'int64' : 'float64';
    else if (typeof v === 'string') k = 'utf8';
    else if (typeof v === 'boolean') k = 'bool';
    else if (v instanceof Uint8Array) k = 'binary';
    else throw new TypeError(`column ${col}: unsupported parameter value ${Object.prototype.toString.call(v)}`);
    if (kind === null) kind = k;
    else if (kind !== k) {
      const nums = new Set<ParamType>([kind, k]);
      if (nums.size === 2 && nums.has('int64') && nums.has('float64')) kind = 'float64';
      else throw new TypeError(`column ${col}: mixes ${kind} and ${k} values; pass explicit types`);
    }
  }
  return kind ?? 'utf8';
}

/**
 * Convert a JS value the way the `pg` driver serializes it, for values that
 * have no direct Arrow parameter type: Date → ISO-8601 text, arrays →
 * PostgreSQL array literal text, other objects → JSON text.
 */
export function normalizeParam(v: any): any {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) return v.toISOString();
  if (v instanceof Uint8Array) return v;
  if (Array.isArray(v)) return pgArrayLiteral(v);
  if (typeof v === 'object') return JSON.stringify(v);
  return v;
}

function pgArrayLiteral(a: any[]): string {
  const items = a.map((x) => {
    if (x === null || x === undefined) return 'NULL';
    if (Array.isArray(x)) return pgArrayLiteral(x);
    const s = typeof x === 'object' && !(x instanceof Date) ? JSON.stringify(x) : String(x instanceof Date ? x.toISOString() : x);
    return '"' + s.replace(/\\/g, '\\\\').replace(/"/g, '\\"') + '"';
  });
  return '{' + items.join(',') + '}';
}
