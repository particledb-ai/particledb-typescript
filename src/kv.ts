import type { Queryable } from './types';

/**
 * Redis-compatible key-value operations for ParticleDB.
 *
 * Provides a familiar Redis-like API backed by ParticleDB's
 * SQL engine for key-value storage and retrieval.
 */
export class KVOps {
  constructor(private pool: Queryable) {}

  /**
   * Set a key-value pair, optionally with a TTL in seconds.
   *
   * @param key - The key to set.
   * @param value - The string value to store.
   * @param ttl - Optional time-to-live in seconds.
   *
   * @example
   * ```ts
   * await db.kv.set('session:123', '{"user":"alice"}', 3600);
   * ```
   */
  async set(key: string, value: string, ttl?: number): Promise<void> {
    if (ttl !== undefined) {
      await this.pool.query('SELECT KV_SET($1, $2, $3)', [key, value, ttl]);
    } else {
      await this.pool.query('SELECT KV_SET($1, $2)', [key, value]);
    }
  }

  /**
   * Get the value for a key.
   *
   * @param key - The key to look up.
   * @returns The value string, or null if the key does not exist.
   *
   * @example
   * ```ts
   * const val = await db.kv.get('session:123');
   * if (val) console.log(JSON.parse(val));
   * ```
   */
  async get(key: string): Promise<string | null> {
    const result = await this.pool.query('SELECT KV_GET($1) AS value', [key]);
    if (result.rows.length === 0 || result.rows[0].value === null) {
      return null;
    }
    return result.rows[0].value;
  }

  /**
   * Delete a key.
   *
   * @param key - The key to delete.
   * @returns True if the key existed and was deleted, false otherwise.
   */
  async delete(key: string): Promise<boolean> {
    const result = await this.pool.query('SELECT KV_DEL($1) AS deleted', [key]);
    // KV_DEL answers the number of keys removed (1 or 0).
    if (result.rows.length === 0) return false;
    const d = result.rows[0].deleted;
    return d === true || Number(d) > 0;
  }

  /**
   * Set multiple key-value pairs atomically.
   *
   * @param mapping - Object mapping keys to values.
   *
   * @example
   * ```ts
   * await db.kv.mset({ 'key1': 'val1', 'key2': 'val2' });
   * ```
   */
  async mset(mapping: Record<string, string>): Promise<void> {
    const keys = Object.keys(mapping);
    const values = Object.values(mapping);
    const args: string[] = [];
    for (let i = 0; i < keys.length; i++) {
      args.push(keys[i], values[i]);
    }

    const placeholders = args.map((_, i) => `$${i + 1}`).join(', ');
    await this.pool.query(`SELECT KV_MSET(${placeholders})`, args);
  }

  /**
   * Get multiple values by their keys.
   *
   * @param keys - Array of keys to look up.
   * @returns Array of values (null for missing keys), in the same order as the input keys.
   */
  async mget(keys: string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    // One KV_GET column per key: the server answers KV_MGET with a single
    // comma-joined string that cannot be split back into values containing
    // a comma.
    const cols = keys.map((_, i) => `KV_GET($${i + 1}) AS v${i}`).join(', ');
    const result = await this.pool.query(`SELECT ${cols}`, keys);
    const row = result.rows[0];
    return keys.map((_, i) => {
      const v = row ? row[`v${i}`] : null;
      return v === null || v === undefined ? null : String(v);
    });
  }

  /**
   * Increment a numeric value stored at key.
   *
   * @param key - The key whose value to increment.
   * @param amount - Amount to increment by. Default: 1.
   * @returns The new value after incrementing.
   */
  async incr(key: string, amount: number = 1): Promise<number> {
    const result = await this.pool.query('SELECT KV_INCR($1, $2) AS value', [key, amount]);
    return result.rows[0].value;
  }

  /**
   * Check whether a key exists.
   *
   * @param key - The key to check.
   * @returns True if the key exists.
   */
  async exists(key: string): Promise<boolean> {
    const result = await this.pool.query('SELECT KV_EXISTS($1) AS exists', [key]);
    return result.rows.length > 0 && result.rows[0].exists === true;
  }

  /**
   * Get the remaining TTL (time-to-live) for a key.
   *
   * @param key - The key to check.
   * @returns Remaining TTL in seconds, or null if no TTL is set or key does not exist.
   */
  async ttl(key: string): Promise<number | null> {
    const result = await this.pool.query('SELECT KV_TTL($1) AS ttl', [key]);
    if (result.rows.length === 0 || result.rows[0].ttl === null) {
      return null;
    }
    return result.rows[0].ttl;
  }

  /**
   * List keys matching a pattern.
   *
   * @param pattern - Glob-style pattern (e.g., 'session:*'). Default: '*' (all keys).
   * @returns Array of matching key names.
   */
  async keys(pattern: string = '*'): Promise<string[]> {
    const result = await this.pool.query('SELECT KV_KEYS($1) AS keys', [pattern]);
    if (result.rows.length === 0 || result.rows[0].keys === null) {
      return [];
    }
    return result.rows[0].keys;
  }
}
