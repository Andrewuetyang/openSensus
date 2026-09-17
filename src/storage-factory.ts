import { SqliteStorage } from "./storage-sqlite.js";
import { PostgresStorage } from "./storage-postgres.js";
import type { SensusStorage } from "./storage.js";

/**
 * Selects the storage backend for a process.
 *
 * The choice is made **once, at startup**. There is deliberately no way to swap
 * backends while running: a connection pool, its schema assumptions and the
 * contract's async shape are all bound for the life of the process, so changing
 * backends means restarting it.
 *
 * `SENSUS_DATABASE_URL` selects PostgreSQL. Anything else, including its
 * absence, uses SQLite at `SENSUS_DB_PATH`.
 */
export interface StorageSelection {
  kind: "postgres" | "sqlite";
  /** The connection string for PostgreSQL, the file path for SQLite. */
  target: string;
}

export function selectStorage(
  environment: NodeJS.ProcessEnv = process.env,
): StorageSelection {
  const databaseUrl = environment.SENSUS_DATABASE_URL?.trim();
  if (databaseUrl) {
    return { kind: "postgres", target: databaseUrl };
  }
  return {
    kind: "sqlite",
    target: environment.SENSUS_DB_PATH?.trim() || "data/sensus.db",
  };
}

export function describeStorage(selection: StorageSelection): string {
  if (selection.kind === "postgres") {
    // The connection string routinely carries a password; never log it whole.
    return `postgres ${redactConnectionString(selection.target)}`;
  }
  return `sqlite ${selection.target}`;
}

export async function openStorageFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): Promise<SensusStorage> {
  return openStorage(selectStorage(environment));
}

export async function openStorage(
  selection: StorageSelection,
): Promise<SensusStorage> {
  if (selection.kind === "postgres") {
    return PostgresStorage.open(selection.target);
  }
  return SqliteStorage.open(selection.target);
}

/** Strips the password from a connection string so it is safe to log. */
export function redactConnectionString(connectionString: string): string {
  try {
    const url = new URL(connectionString);
    if (url.password) url.password = "***";
    return url.toString();
  } catch {
    // Not a URL we can parse; report nothing rather than risk a secret.
    return "(unparseable connection string)";
  }
}
