/**
 * The SDK's single set of SQL text encoders. Values go to the server as bound
 * parameters wherever the statement allows one; these exist for the places
 * that cannot take a parameter (identifiers, DDL options).
 */

/** Quote a SQL identifier by wrapping in double quotes with internal escaping. */
export function quoteIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"';
}

/**
 * Escape the body of a standard-conforming string literal ('...') by doubling
 * single quotes. The caller adds the surrounding quotes and must never use it
 * inside an E'...' literal (where a backslash would escape the next quote).
 */
export function escapeLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

const SIMPLE_IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;
const QUALIFIED_IDENT = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/;

/**
 * Refuse anything but an unquoted SQL identifier (`qualified` also allows
 * `schema.table`). Used where the SDK splices a caller's table / column name
 * into SQL text as-is, so it keeps the server's case folding.
 */
export function assertIdentifier(value: unknown, qualified: boolean, message: string): asserts value is string {
  if (typeof value !== 'string' || !(qualified ? QUALIFIED_IDENT : SIMPLE_IDENT).test(value)) {
    throw new TypeError(message);
  }
}
