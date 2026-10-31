import type { Queryable } from './types';

/**
 * AI function operations for ParticleDB.
 *
 * Provides methods for embedding generation, text generation,
 * classification, and summarization using ParticleDB's built-in AI functions.
 */
export class AIOps {
  constructor(private pool: Queryable) {}

  /**
   * Generate a vector embedding for a text string.
   *
   * @param text - The text to embed.
   * @param model - Optional model name to use for embedding.
   * @returns The embedding as an array of numbers.
   *
   * @example
   * ```ts
   * const embedding = await db.ai.embed('Hello world');
   * console.log(embedding.length); // e.g. 1536
   * ```
   */
  async embed(text: string, model?: string): Promise<number[]> {
    let sql: string;
    const params: any[] = [text];

    if (model) {
      sql = 'SELECT AI_EMBED($1, $2) AS embedding';
      params.push(model);
    } else {
      sql = 'SELECT AI_EMBED($1) AS embedding';
    }

    const result = await this.pool.query(sql, params);
    return result.rows[0].embedding;
  }

  /**
   * Generate embeddings for multiple texts in a single batch call.
   *
   * @param texts - Array of text strings to embed.
   * @param model - Optional model name to use for embedding.
   * @returns Array of embeddings, one per input text.
   *
   * @example
   * ```ts
   * const embeddings = await db.ai.embedBatch(['Hello', 'World']);
   * console.log(embeddings.length); // 2
   * ```
   */
  async embedBatch(texts: string[], model?: string): Promise<number[][]> {
    let sql: string;
    const params: any[] = [JSON.stringify(texts)];

    if (model) {
      sql = 'SELECT AI_EMBED_BATCH($1, $2) AS embeddings';
      params.push(model);
    } else {
      sql = 'SELECT AI_EMBED_BATCH($1) AS embeddings';
    }

    const result = await this.pool.query(sql, params);
    return result.rows[0].embeddings;
  }

  /**
   * Generate text from a prompt using ParticleDB's AI text generation.
   *
   * @param prompt - The input prompt.
   * @param model - Optional model name to use.
   * @returns The generated text response.
   *
   * @example
   * ```ts
   * const response = await db.ai.generate('Explain vector databases');
   * ```
   */
  async generate(prompt: string, model?: string): Promise<string> {
    let sql: string;
    const params: any[] = [prompt];

    if (model) {
      sql = 'SELECT AI_GENERATE($1, $2) AS text';
      params.push(model);
    } else {
      sql = 'SELECT AI_GENERATE($1) AS text';
    }

    const result = await this.pool.query(sql, params);
    return result.rows[0].text;
  }

  /**
   * Classify text into one of the provided labels.
   *
   * @param text - The text to classify.
   * @param labels - Array of possible classification labels.
   * @returns The predicted label.
   *
   * @example
   * ```ts
   * const label = await db.ai.classify(
   *   'I love this product!',
   *   ['positive', 'negative', 'neutral']
   * );
   * ```
   */
  async classify(text: string, labels: string[]): Promise<string> {
    const sql = 'SELECT AI_CLASSIFY($1, $2) AS label';
    const result = await this.pool.query(sql, [text, JSON.stringify(labels)]);
    return result.rows[0].label;
  }

  /**
   * Summarize a block of text.
   *
   * @param text - The text to summarize.
   * @returns A summary of the input text.
   *
   * @example
   * ```ts
   * const summary = await db.ai.summarize(longArticle);
   * ```
   */
  async summarize(text: string): Promise<string> {
    const sql = 'SELECT AI_SUMMARIZE($1) AS summary';
    const result = await this.pool.query(sql, [text]);
    return result.rows[0].summary;
  }
}
