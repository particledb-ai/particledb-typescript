import { EventEmitter } from 'events';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/**
 * Options for creating a StreamingClient.
 */
export interface StreamingConfig {
  /** Hostname of the ParticleDB server. Default: 'localhost' */
  host?: string;

  /**
   * HTTP port of the ParticleDB server (used for SSE connections
   * and as the WebSocket upgrade target). Default: 4000
   */
  httpPort?: number;

  /**
   * Transport to use for streaming connections.
   * - 'websocket': Bidirectional WebSocket (requires 'ws' package in Node.js)
   * - 'sse': Server-Sent Events over HTTP (works anywhere with fetch)
   *
   * Default: 'websocket'
   */
  transport?: 'websocket' | 'sse';

  /** Whether to use TLS (wss:// / https://). Default: false */
  ssl?: boolean;

  /**
   * Automatically reconnect on disconnect.
   * Applies to both WebSocket and SSE transports. Default: true
   */
  autoReconnect?: boolean;

  /** Maximum number of reconnection attempts. Default: 10 */
  maxReconnectAttempts?: number;

  /** Base delay between reconnection attempts in ms (doubled each retry). Default: 1000 */
  reconnectBaseDelay?: number;
}

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

/**
 * A CDC (Change Data Capture) event received from the server.
 */
export interface ChangeEvent {
  /** Event type: 'INSERT', 'UPDATE', or 'DELETE'. */
  event: string;
  /** Table that was modified. */
  table: string;
  /** Number of rows affected. */
  rows: number;
  /** JSON-encoded payload with the changed data. */
  payload: string;
}

/**
 * A single row yielded during query streaming.
 */
export type StreamRow = Record<string, any>;

/**
 * Callback invoked for each CDC / pub-sub message on a subscribed channel.
 */
export type SubscriptionCallback = (event: ChangeEvent) => void;

/**
 * Callback invoked for each row during query streaming.
 */
export type RowCallback = (row: StreamRow) => void;

/**
 * Generic event handler callback.
 */
export type EventHandler = (data: any) => void;

// ---------------------------------------------------------------------------
// StreamingClient
// ---------------------------------------------------------------------------

/**
 * Streaming client for ParticleDB using WebSocket or Server-Sent Events.
 *
 * Provides real-time CDC/pub-sub subscriptions, row-by-row query streaming,
 * and a general-purpose event system. Supports automatic reconnection with
 * exponential backoff.
 *
 * **WebSocket transport** connects to `ws(s)://host:httpPort/ws` and sends
 * JSON-framed commands. This is the recommended transport for bidirectional
 * communication and CDC subscriptions.
 *
 * **SSE transport** connects to `http(s)://host:httpPort/v1/stream` for
 * server-push events. It is read-only and uses `POST /v1/sql` for issuing
 * queries whose results are then pushed over the SSE channel.
 *
 * @example
 * ```ts
 * import { StreamingClient } from '@particledb/sdk';
 *
 * const client = new StreamingClient({ host: 'localhost', httpPort: 4000 });
 * await client.connect();
 *
 * // Subscribe to CDC events
 * client.subscribe('cdc_users', (event) => {
 *   console.log(`${event.event} on ${event.table}: ${event.payload}`);
 * });
 *
 * // Stream query results row-by-row
 * for await (const row of client.streamQuery('SELECT * FROM large_table')) {
 *   process.stdout.write(JSON.stringify(row) + '\n');
 * }
 *
 * await client.close();
 * ```
 */
export class StreamingClient {
  private config: Required<StreamingConfig>;
  private emitter: EventEmitter;
  private ws: any | null = null;
  private eventSource: any | null = null;
  private subscriptions: Map<string, SubscriptionCallback[]> = new Map();
  private eventHandlers: Map<string, EventHandler[]> = new Map();
  private connected = false;
  private reconnectAttempts = 0;
  private closed = false;

  constructor(config?: StreamingConfig) {
    this.config = {
      host: config?.host ?? 'localhost',
      httpPort: config?.httpPort ?? 4000,
      transport: config?.transport ?? 'websocket',
      ssl: config?.ssl ?? false,
      autoReconnect: config?.autoReconnect ?? true,
      maxReconnectAttempts: config?.maxReconnectAttempts ?? 10,
      reconnectBaseDelay: config?.reconnectBaseDelay ?? 1000,
    };
    this.emitter = new EventEmitter();
  }

  // -------------------------------------------------------------------------
  // Connection lifecycle
  // -------------------------------------------------------------------------

  /**
   * Open the streaming connection.
   *
   * For the WebSocket transport this establishes a persistent bidirectional
   * channel. For SSE it opens an `EventSource` connection to the server's
   * streaming endpoint.
   *
   * @throws If the connection cannot be established.
   */
  async connect(): Promise<void> {
    this.closed = false;

    if (this.config.transport === 'websocket') {
      await this.connectWebSocket();
    } else {
      await this.connectSSE();
    }
  }

  /**
   * Close the streaming connection and release all resources.
   *
   * After calling `close()` the client must not be reused. All active
   * subscriptions are cancelled and pending event handlers are removed.
   */
  async close(): Promise<void> {
    this.closed = true;
    this.connected = false;
    this.subscriptions.clear();
    this.eventHandlers.clear();

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }

    this.emitter.removeAllListeners();
  }

  /** Whether the streaming connection is currently open. */
  get isConnected(): boolean {
    return this.connected;
  }

  // -------------------------------------------------------------------------
  // Subscriptions
  // -------------------------------------------------------------------------

  /**
   * Subscribe to CDC or pub-sub events on a channel.
   *
   * Events are delivered to the `callback` as {@link ChangeEvent} objects.
   * The channel name typically follows the convention `cdc_<table>` for
   * CDC events or any arbitrary string for application-level pub/sub.
   *
   * @param channel - Channel name to subscribe to (e.g., `'cdc_users'`).
   * @param callback - Function called with each received event.
   * @returns An unsubscribe function. Call it to stop receiving events.
   *
   * @example
   * ```ts
   * const unsub = client.subscribe('cdc_orders', (event) => {
   *   console.log('Order changed:', event.payload);
   * });
   *
   * // Later...
   * unsub();
   * ```
   */
  subscribe(channel: string, callback: SubscriptionCallback): () => void {
    const existing = this.subscriptions.get(channel);
    if (existing) {
      existing.push(callback);
    } else {
      this.subscriptions.set(channel, [callback]);
    }

    // Tell the server about the subscription.
    this.sendCommand({ type: 'subscribe', channel });

    return () => {
      const cbs = this.subscriptions.get(channel);
      if (cbs) {
        const idx = cbs.indexOf(callback);
        if (idx !== -1) cbs.splice(idx, 1);
        if (cbs.length === 0) {
          this.subscriptions.delete(channel);
          this.sendCommand({ type: 'unsubscribe', channel });
        }
      }
    };
  }

  // -------------------------------------------------------------------------
  // Query streaming
  // -------------------------------------------------------------------------

  /**
   * Execute a SQL query and stream the results row by row.
   *
   * Returns an async iterable that yields one {@link StreamRow} per
   * result row. The query is executed on the server and rows are pushed
   * over the active streaming connection as they become available.
   *
   * @param sql - The SQL statement to execute.
   * @param params - Optional positional parameters for the statement.
   * @returns An async iterable of row objects.
   *
   * @example
   * ```ts
   * for await (const row of client.streamQuery('SELECT * FROM events')) {
   *   console.log(row);
   * }
   * ```
   */
  async *streamQuery(
    sql: string,
    params?: any[]
  ): AsyncIterableIterator<StreamRow> {
    const queryId = `q_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

    if (this.config.transport === 'websocket') {
      yield* this.streamQueryWebSocket(queryId, sql, params);
    } else {
      yield* this.streamQuerySSE(sql, params);
    }
  }

  // -------------------------------------------------------------------------
  // General event handlers
  // -------------------------------------------------------------------------

  /**
   * Register a handler for a specific event type.
   *
   * This is a lower-level API than {@link subscribe}. The server may emit
   * typed events such as `'connected'`, `'error'`, `'heartbeat'`, or
   * application-defined event types.
   *
   * @param type - Event type string.
   * @param handler - Function called with the event data.
   * @returns An unsubscribe function.
   *
   * @example
   * ```ts
   * client.onEvent('error', (err) => console.error('Stream error:', err));
   * client.onEvent('heartbeat', () => console.log('Server alive'));
   * ```
   */
  onEvent(type: string, handler: EventHandler): () => void {
    const existing = this.eventHandlers.get(type);
    if (existing) {
      existing.push(handler);
    } else {
      this.eventHandlers.set(type, [handler]);
    }

    this.emitter.on(type, handler);

    return () => {
      const handlers = this.eventHandlers.get(type);
      if (handlers) {
        const idx = handlers.indexOf(handler);
        if (idx !== -1) handlers.splice(idx, 1);
        if (handlers.length === 0) {
          this.eventHandlers.delete(type);
        }
      }
      this.emitter.off(type, handler);
    };
  }

  // =========================================================================
  // Private - WebSocket transport
  // =========================================================================

  private async connectWebSocket(): Promise<void> {
    const scheme = this.config.ssl ? 'wss' : 'ws';
    const url = `${scheme}://${this.config.host}:${this.config.httpPort}/ws`;

    // Use the global WebSocket in browsers. In Node.js, dynamically require
    // the 'ws' package (optional peer dependency).
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const WS: typeof globalThis.WebSocket =
      typeof WebSocket !== 'undefined'
        ? WebSocket
        : (require as any)('ws') as any;

    return new Promise<void>((resolve, reject) => {
      const ws = new WS(url);
      this.ws = ws;

      ws.onopen = () => {
        this.connected = true;
        this.reconnectAttempts = 0;
        this.emitter.emit('connected', { transport: 'websocket', url });
        resolve();
      };

      ws.onmessage = (event: MessageEvent | { data: any }) => {
        this.handleMessage(
          typeof event.data === 'string' ? event.data : String(event.data)
        );
      };

      ws.onerror = (err: any) => {
        this.emitter.emit('error', err);
        if (!this.connected) {
          reject(new Error(`WebSocket connection failed: ${url}`));
        }
      };

      ws.onclose = () => {
        this.connected = false;
        this.emitter.emit('disconnected', { transport: 'websocket' });
        if (!this.closed && this.config.autoReconnect) {
          this.scheduleReconnect();
        }
      };
    });
  }

  private sendCommand(cmd: Record<string, any>): void {
    if (this.ws && this.connected) {
      this.ws.send(JSON.stringify(cmd));
    }
  }

  private handleMessage(raw: string): void {
    let msg: any;
    try {
      msg = JSON.parse(raw);
    } catch {
      // Non-JSON frame - ignore or emit as raw.
      this.emitter.emit('raw', raw);
      return;
    }

    // Route the message based on its `type` field.
    const type: string | undefined = msg.type;

    if (type === 'event' || type === 'cdc') {
      const channel: string | undefined = msg.channel;
      const changeEvent: ChangeEvent = {
        event: msg.event ?? type,
        table: msg.table ?? '',
        rows: msg.rows ?? 0,
        payload: typeof msg.payload === 'string' ? msg.payload : JSON.stringify(msg.payload ?? {}),
      };

      if (channel) {
        const callbacks = this.subscriptions.get(channel);
        if (callbacks) {
          for (const cb of callbacks) cb(changeEvent);
        }
      }

      // Also emit to general event handlers.
      this.emitter.emit('event', changeEvent);
    } else if (type === 'row') {
      // Streamed query row - forwarded via the emitter keyed on queryId.
      this.emitter.emit(`row:${msg.queryId}`, msg.data);
    } else if (type === 'end') {
      this.emitter.emit(`end:${msg.queryId}`);
    } else if (type === 'error') {
      this.emitter.emit('error', msg);
      if (msg.queryId) {
        this.emitter.emit(`error:${msg.queryId}`, msg);
      }
    } else if (type === 'heartbeat' || type === 'ping') {
      this.emitter.emit('heartbeat', msg);
    } else if (type) {
      // Application-defined event type.
      this.emitter.emit(type, msg);
    }
  }

  private async *streamQueryWebSocket(
    queryId: string,
    sql: string,
    params?: any[]
  ): AsyncIterableIterator<StreamRow> {
    // Send the query command.
    this.sendCommand({ type: 'query', queryId, sql, params });

    // Yield rows as they arrive via the internal emitter.
    const rowQueue: StreamRow[] = [];
    let done = false;
    let error: Error | null = null;
    let resolve: (() => void) | null = null;

    const onRow = (data: StreamRow) => {
      rowQueue.push(data);
      if (resolve) {
        resolve();
        resolve = null;
      }
    };

    const onEnd = () => {
      done = true;
      if (resolve) {
        resolve();
        resolve = null;
      }
    };

    const onError = (err: any) => {
      error = new Error(err.message ?? 'Stream query error');
      done = true;
      if (resolve) {
        resolve();
        resolve = null;
      }
    };

    this.emitter.on(`row:${queryId}`, onRow);
    this.emitter.on(`end:${queryId}`, onEnd);
    this.emitter.on(`error:${queryId}`, onError);

    try {
      while (true) {
        while (rowQueue.length > 0) {
          yield rowQueue.shift()!;
        }
        if (done) break;
        // Wait for the next row or end signal.
        await new Promise<void>((r) => {
          resolve = r;
        });
        if (error) throw error;
      }
    } finally {
      this.emitter.off(`row:${queryId}`, onRow);
      this.emitter.off(`end:${queryId}`, onEnd);
      this.emitter.off(`error:${queryId}`, onError);
    }
  }

  // =========================================================================
  // Private - SSE transport
  // =========================================================================

  private async connectSSE(): Promise<void> {
    const scheme = this.config.ssl ? 'https' : 'http';
    const url = `${scheme}://${this.config.host}:${this.config.httpPort}/v1/stream`;

    // Use the native EventSource when available (browsers and Node >= 22),
    // or fall back to the 'eventsource' npm package (optional peer dep).
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const ES: typeof globalThis.EventSource =
      typeof EventSource !== 'undefined'
        ? EventSource
        : (require as any)('eventsource') as any;

    return new Promise<void>((resolve, reject) => {
      const es = new ES(url);
      this.eventSource = es;

      es.onopen = () => {
        this.connected = true;
        this.reconnectAttempts = 0;
        this.emitter.emit('connected', { transport: 'sse', url });
        resolve();
      };

      es.onerror = (err: any) => {
        this.emitter.emit('error', err);
        if (!this.connected) {
          es.close();
          reject(new Error(`SSE connection failed: ${url}`));
          return;
        }
        // EventSource reconnects automatically, but we track state.
        this.connected = false;
        this.emitter.emit('disconnected', { transport: 'sse' });
      };

      // Default "message" event.
      es.onmessage = (event: MessageEvent) => {
        this.handleSSEMessage('message', event.data);
      };

      // Named server events.
      for (const eventName of ['cdc', 'row', 'end', 'heartbeat', 'error']) {
        es.addEventListener(eventName, ((event: MessageEvent) => {
          this.handleSSEMessage(eventName, event.data);
        }) as EventListener);
      }
    });
  }

  private handleSSEMessage(type: string, rawData: string): void {
    let data: any;
    try {
      data = JSON.parse(rawData);
    } catch {
      data = rawData;
    }

    if (type === 'cdc' || type === 'message') {
      if (data && typeof data === 'object' && data.channel) {
        const changeEvent: ChangeEvent = {
          event: data.event ?? type,
          table: data.table ?? '',
          rows: data.rows ?? 0,
          payload: typeof data.payload === 'string'
            ? data.payload
            : JSON.stringify(data.payload ?? {}),
        };

        const callbacks = this.subscriptions.get(data.channel);
        if (callbacks) {
          for (const cb of callbacks) cb(changeEvent);
        }
        this.emitter.emit('event', changeEvent);
      }
    } else if (type === 'row') {
      this.emitter.emit(`row:${data?.queryId}`, data?.data ?? data);
    } else if (type === 'end') {
      this.emitter.emit(`end:${data?.queryId}`);
    } else if (type === 'error') {
      this.emitter.emit('error', data);
    } else if (type === 'heartbeat') {
      this.emitter.emit('heartbeat', data);
    }

    // Forward to general event handlers.
    this.emitter.emit(type, data);
  }

  /**
   * SSE query streaming falls back to HTTP POST for query execution,
   * then reads the response body as newline-delimited JSON rows.
   */
  private async *streamQuerySSE(
    sql: string,
    params?: any[]
  ): AsyncIterableIterator<StreamRow> {
    const scheme = this.config.ssl ? 'https' : 'http';
    const url = `${scheme}://${this.config.host}:${this.config.httpPort}/v1/sql`;

    const body = JSON.stringify({ sql, params });

    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Query failed (${response.status}): ${text}`);
    }

    const json: any = await response.json();
    const rows: StreamRow[] = json.rows ?? [];

    for (const row of rows) {
      yield row;
    }
  }

  // =========================================================================
  // Private - reconnection
  // =========================================================================

  private scheduleReconnect(): void {
    if (
      this.closed ||
      this.reconnectAttempts >= this.config.maxReconnectAttempts
    ) {
      this.emitter.emit('error', {
        message: 'Maximum reconnection attempts reached',
        attempts: this.reconnectAttempts,
      });
      return;
    }

    const delay =
      this.config.reconnectBaseDelay * Math.pow(2, this.reconnectAttempts);
    this.reconnectAttempts++;

    this.emitter.emit('reconnecting', {
      attempt: this.reconnectAttempts,
      delay,
    });

    setTimeout(async () => {
      if (this.closed) return;
      try {
        await this.connect();
        // Re-subscribe to all active channels after reconnect.
        for (const channel of this.subscriptions.keys()) {
          this.sendCommand({ type: 'subscribe', channel });
        }
      } catch {
        // connect() will reject - the close handler will trigger another retry.
      }
    }, delay);
  }
}
