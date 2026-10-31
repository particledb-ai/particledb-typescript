import type { Queryable } from './types';
import { RAGPipelineConfig, RAGQueryOptions } from './types';
import { assertIdentifier } from './sql';

/**
 * RAG (Retrieval-Augmented Generation) query operations for ParticleDB.
 *
 * Provides methods to execute RAG queries that combine vector similarity
 * search with text retrieval, and to manage named RAG pipelines.
 */
export class RAGOps {
  constructor(private pool: Queryable) {}

  /**
   * Execute a RAG query against ParticleDB.
   *
   * Either specify a named pipeline or provide table/column details directly.
   *
   * @param question - The natural language question to answer.
   * @param options - Query options specifying the pipeline or table configuration.
   * @returns The generated answer string.
   *
   * @example
   * ```ts
   * // Using a named pipeline
   * const answer = await db.rag.query('What is ParticleDB?',
   *   { pipeline: 'docs_pipeline' });
   *
   * // Using inline table config
   * const answer = await db.rag.query('What is ParticleDB?', {
   *   table: 'documents',
   *   vectorCol: 'embedding',
   *   textCol: 'content',
   *   k: 5
   * });
   * ```
   */
  async query(question: string, options?: RAGQueryOptions): Promise<string> {
    let sql: string;
    const params: any[] = [question];

    if (options?.pipeline) {
      sql = 'SELECT RAG($1, $2) AS answer';
      params.push(options.pipeline);
    } else {
      const table = options?.table ?? 'documents';
      const vectorCol = options?.vectorCol ?? 'embedding';
      const textCol = options?.textCol ?? 'content';
      const k = options?.k ?? 5;

      sql = `SELECT RAG_QUERY($1, $2, $3, $4, $5) AS answer`;
      params.push(table, vectorCol, textCol, k);
    }

    const result = await this.pool.query(sql, params);
    return result.rows[0].answer;
  }

  /**
   * Create a named RAG pipeline for reuse.
   *
   * @param name - Unique name for the pipeline.
   * @param config - Pipeline configuration specifying table, columns, and parameters.
   *
   * @example
   * ```ts
   * await db.rag.createPipeline('docs_pipeline', {
   *   table: 'documents',
   *   vectorCol: 'embedding',
   *   textCol: 'content',
   *   k: 5,
   *   mode: 'vector'
   * });
   * ```
   */
  async createPipeline(name: string, config: RAGPipelineConfig): Promise<void> {
    const k = config.k ?? 5;
    const mode = config.mode === 'simple' ? 'vector' : (config.mode ?? 'vector');

    for (const [value, qualified] of [[name, false], [config.table, true], [config.vectorCol, false], [config.textCol, false]] as const) {
      assertIdentifier(value, qualified, 'pipeline names and columns must be simple SQL identifiers');
    }
    if (!Number.isSafeInteger(k) || k <= 0) throw new RangeError('k must be a positive integer');
    if (mode !== 'vector' && mode !== 'hybrid') throw new TypeError('mode must be vector or hybrid');
    await this.pool.query(`CREATE RAG PIPELINE ${name} TABLE ${config.table} VECTOR_COLUMN ${config.vectorCol} TEXT_COLUMN ${config.textCol} TOP_K ${k} MODE ${mode}`);
  }
}
