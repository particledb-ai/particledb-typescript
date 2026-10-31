import type { Queryable } from './types';
import { escapeLiteral, quoteIdent } from './sql';

/**
 * Extended database operations for ParticleDB.
 *
 * Provides methods for time travel queries, materialized views,
 * changefeeds/CDC, geospatial functions, time-series operations,
 * multi-tenant namespace management, and lakehouse file access.
 *
 * @example
 * ```ts
 * // Time travel
 * const rows = await db.ops.queryAsOf('SELECT * FROM orders', '2024-01-01T00:00:00Z');
 *
 * // Materialized views
 * await db.ops.createMaterializedView('active_users', 'SELECT * FROM users WHERE active');
 * await db.ops.refreshMaterializedView('active_users');
 *
 * // Lakehouse
 * const data = await db.ops.readParquet('/data/events.parquet');
 * ```
 */
/**
 * The message the deprecated namespace methods fail with.
 *
 * ParticleDB has no server-side namespace surface: `CREATE NAMESPACE`,
 * `SET NAMESPACE` and `DROP NAMESPACE` are refused with a parse error on every
 * wire, so those calls can never succeed.
 */
function namespaceUnsupported(verb: string): string {
  return (
    `${verb} is not implemented by any ParticleDB server: there is no namespace ` +
    'surface on any wire, so this call can never succeed. Use a table-name ' +
    'prefix, a tenant column filtered on every query, or a separate server per ' +
    'tenant. See https://particledb.ai/getting-started/databases-and-schemas/'
  );
}

export class ExtendedOps {
  constructor(private pool: Queryable) {}

  // ── Time Travel ─────────────────────────────────────────────────────

  /**
   * Execute a query as of a historical timestamp.
   *
   * @param sql - The SQL query to execute.
   * @param timestamp - ISO-8601 timestamp (e.g., '2024-01-01T00:00:00Z').
   * @param params - Optional parameter values.
   * @returns The query result rows.
   */
  async queryAsOf<T = Record<string, any>>(
    sql: string,
    timestamp: string,
    params?: any[]
  ): Promise<T[]> {
    const asOfSql = `SET TIMESTAMP AS OF '${escapeLiteral(timestamp)}'; ${sql}`;
    const result = await this.pool.query(asOfSql, params);
    return result.rows as T[];
  }

  // ── Materialized Views ──────────────────────────────────────────────

  /**
   * Create a materialized view.
   *
   * @param name - Name for the materialized view.
   * @param query - The SELECT query defining the view contents.
   * @param refresh - Refresh strategy: 'manual' (default) or an interval like '1h'.
   * @returns Number of rows affected.
   */
  async createMaterializedView(
    name: string,
    query: string,
    refresh: string = 'manual'
  ): Promise<number> {
    let sql: string;
    if (refresh === 'manual') {
      sql = `CREATE MATERIALIZED VIEW ${quoteIdent(name)} AS ${query}`;
    } else {
      sql = `CREATE MATERIALIZED VIEW ${quoteIdent(name)} WITH (refresh_interval = '${escapeLiteral(refresh)}') AS ${query}`;
    }
    const result = await this.pool.query(sql);
    return result.rowCount ?? 0;
  }

  /**
   * Trigger a refresh of a materialized view.
   *
   * @param name - Name of the materialized view to refresh.
   * @returns Number of rows affected.
   */
  async refreshMaterializedView(name: string): Promise<number> {
    const result = await this.pool.query(`REFRESH MATERIALIZED VIEW ${quoteIdent(name)}`);
    return result.rowCount ?? 0;
  }

  /**
   * Drop a materialized view.
   *
   * @param name - Name of the materialized view to drop.
   * @returns Number of rows affected.
   */
  async dropMaterializedView(name: string): Promise<number> {
    const result = await this.pool.query(
      `DROP MATERIALIZED VIEW IF EXISTS ${quoteIdent(name)}`
    );
    return result.rowCount ?? 0;
  }

  // ── Changefeeds / CDC ───────────────────────────────────────────────

  /**
   * Create a changefeed for change data capture.
   *
   * @param name - Name for the changefeed.
   * @param table - Source table to watch.
   * @param sinkType - Sink type (e.g., 'kafka', 'webhook', 's3').
   * @param sinkUrl - Sink destination URL.
   * @returns Number of rows affected.
   */
  async createChangefeed(
    name: string,
    table: string,
    sinkType: string,
    sinkUrl: string
  ): Promise<number> {
    const sql = `CREATE CHANGEFEED ${quoteIdent(name)} ON ${quoteIdent(table)} INTO '${escapeLiteral(sinkType)}' WITH (sink_url = '${escapeLiteral(sinkUrl)}')`;
    const result = await this.pool.query(sql);
    return result.rowCount ?? 0;
  }

  /**
   * Pause a running changefeed.
   *
   * @param name - Name of the changefeed to pause.
   * @returns Number of rows affected.
   */
  async pauseChangefeed(name: string): Promise<number> {
    const result = await this.pool.query(
      `ALTER CHANGEFEED ${quoteIdent(name)} PAUSE`
    );
    return result.rowCount ?? 0;
  }

  /**
   * Resume a paused changefeed.
   *
   * @param name - Name of the changefeed to resume.
   * @returns Number of rows affected.
   */
  async resumeChangefeed(name: string): Promise<number> {
    const result = await this.pool.query(
      `ALTER CHANGEFEED ${quoteIdent(name)} RESUME`
    );
    return result.rowCount ?? 0;
  }

  /**
   * Drop a changefeed.
   *
   * @param name - Name of the changefeed to drop.
   * @returns Number of rows affected.
   */
  async dropChangefeed(name: string): Promise<number> {
    const result = await this.pool.query(
      `DROP CHANGEFEED IF EXISTS ${quoteIdent(name)}`
    );
    return result.rowCount ?? 0;
  }

  /**
   * List all changefeeds.
   *
   * @returns Array of changefeed metadata rows.
   */
  async listChangefeeds(): Promise<Record<string, any>[]> {
    const result = await this.pool.query('SHOW CHANGEFEEDS');
    return result.rows;
  }

  // ── Geospatial ──────────────────────────────────────────────────────

  /**
   * Compute the Haversine distance between two geographic points.
   *
   * @param p1Lon - Longitude of the first point.
   * @param p1Lat - Latitude of the first point.
   * @param p2Lon - Longitude of the second point.
   * @param p2Lat - Latitude of the second point.
   * @returns Distance in meters.
   */
  async stDistance(
    p1Lon: number,
    p1Lat: number,
    p2Lon: number,
    p2Lat: number
  ): Promise<number> {
    const sql = `SELECT ST_DISTANCE(ST_POINT(${Number(p1Lon)}, ${Number(p1Lat)}), ST_POINT(${Number(p2Lon)}, ${Number(p2Lat)})) AS distance`;
    const result = await this.pool.query(sql);
    return parseFloat(result.rows[0].distance);
  }

  /**
   * Test whether a point falls within a polygon.
   *
   * @param pointLon - Longitude of the point.
   * @param pointLat - Latitude of the point.
   * @param polygonWkt - Polygon in WKT format.
   * @returns True if the point is within the polygon.
   */
  async stWithin(
    pointLon: number,
    pointLat: number,
    polygonWkt: string
  ): Promise<boolean> {
    const sql = `SELECT ST_WITHIN(ST_POINT(${Number(pointLon)}, ${Number(pointLat)}), ST_GEOMFROMTEXT('${escapeLiteral(polygonWkt)}')) AS result`;
    const result = await this.pool.query(sql);
    const val = result.rows[0]?.result;
    return val === true || val === 'true' || val === 't';
  }

  // ── Time-Series ─────────────────────────────────────────────────────

  /**
   * Create a data retention policy for a time-series table.
   *
   * @param name - Name for the retention policy.
   * @param table - Target table.
   * @param column - Timestamp column for age-based retention.
   * @param retention - Retention period (e.g., '30d', '1y', '90d').
   * @returns Number of rows affected.
   */
  async createRetentionPolicy(
    name: string,
    table: string,
    column: string,
    retention: string
  ): Promise<number> {
    const sql = `CREATE RETENTION POLICY ${quoteIdent(name)} ON ${quoteIdent(table)} USING (${quoteIdent(column)}) RETAIN FOR '${escapeLiteral(retention)}'`;
    const result = await this.pool.query(sql);
    return result.rowCount ?? 0;
  }

  /**
   * Create a continuous aggregate for time-series data.
   *
   * @param name - Name for the continuous aggregate.
   * @param query - The aggregation query.
   * @param interval - Refresh interval (e.g., '1m', '5m', '1h').
   * @returns Number of rows affected.
   */
  async createContinuousAggregate(
    name: string,
    query: string,
    interval: string
  ): Promise<number> {
    const sql = `CREATE CONTINUOUS AGGREGATE ${quoteIdent(name)} WITH (refresh_interval = '${escapeLiteral(interval)}') AS ${query}`;
    const result = await this.pool.query(sql);
    return result.rowCount ?? 0;
  }

  // ── Multi-Tenant / Namespaces - NOT IMPLEMENTED SERVER-SIDE ─────────
  //
  // DEPRECATED 2026-09-03. ParticleDB has no server-side namespace surface.
  // CREATE NAMESPACE, SET NAMESPACE and DROP NAMESPACE are refused with a
  // parse error on every wire (PG, native and MySQL), and the tenancy model
  // that would give them a meaning is still an open decision - all three
  // candidate models leave these verbs refused. The three methods below used
  // to emit that SQL and document a row count on success, an outcome that
  // could not occur. They now throw without contacting the server, and are
  // kept only so existing code keeps compiling; they will be removed in the
  // next major version.
  //
  // For tenant isolation today: a table-name prefix, a tenant column filtered
  // on every query, or a separate server per tenant (its own --data-dir and
  // ports). See https://particledb.ai/getting-started/databases-and-schemas/

  /**
   * Always throws - `CREATE NAMESPACE` is not implemented server-side.
   *
   * @deprecated 2026-09-03 - this never worked. No ParticleDB server accepts
   * `CREATE NAMESPACE` on any wire. Throws without contacting the server.
   *
   * @param name - Ignored; no request is sent.
   * @param quotas - Ignored; no request is sent.
   * @throws Always.
   */
  async createNamespace(
    name: string,
    quotas?: Record<string, any>
  ): Promise<number> {
    throw new Error(namespaceUnsupported('CREATE NAMESPACE'));
  }

  /**
   * Always throws - `SET NAMESPACE` is not implemented server-side.
   *
   * @deprecated 2026-09-03 - this never worked. No ParticleDB server accepts
   * `SET NAMESPACE` on any wire. Throws without contacting the server.
   *
   * @param name - Ignored; no request is sent.
   * @throws Always.
   */
  async setNamespace(name: string): Promise<number> {
    throw new Error(namespaceUnsupported('SET NAMESPACE'));
  }

  /**
   * Always throws - `DROP NAMESPACE` is not implemented server-side.
   *
   * @deprecated 2026-09-03 - this never worked. No ParticleDB server accepts
   * `DROP NAMESPACE` on any wire. Throws without contacting the server.
   *
   * @param name - Ignored; no request is sent.
   * @throws Always.
   */
  async dropNamespace(name: string): Promise<number> {
    throw new Error(namespaceUnsupported('DROP NAMESPACE'));
  }

  // ── Lakehouse ───────────────────────────────────────────────────────

  /**
   * Query a Parquet file directly.
   *
   * @param path - Path to the Parquet file (local or remote URI).
   * @returns The query result rows.
   */
  async readParquet<T = Record<string, any>>(path: string): Promise<T[]> {
    const sql = `SELECT * FROM read_parquet('${escapeLiteral(path)}')`;
    const result = await this.pool.query(sql);
    return result.rows as T[];
  }

  /**
   * Query a CSV file directly.
   *
   * @param path - Path to the CSV file (local or remote URI).
   * @returns The query result rows.
   */
  async readCsv<T = Record<string, any>>(path: string): Promise<T[]> {
    const sql = `SELECT * FROM read_csv('${escapeLiteral(path)}')`;
    const result = await this.pool.query(sql);
    return result.rows as T[];
  }
}
