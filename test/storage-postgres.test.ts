import { describe, it } from "node:test";
import { Pool } from "pg";
import { runStorageConformance } from "./conformance.js";
import { PostgresStorage } from "../src/storage-postgres.js";

/**
 * The suite runs against a real PostgreSQL instance. Without one it reports a
 * skip rather than a failure, so `npm test` stays green on a machine that has
 * no database — but the skip is visible, not silent.
 */
const connectionString =
  process.env.SENSUS_TEST_DATABASE_URL ??
  `postgres://${process.env.USER ?? "postgres"}@127.0.0.1:5432/sensus_test`;

async function reachable(): Promise<boolean> {
  const pool = new Pool({ connectionString, connectionTimeoutMillis: 2000 });
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  } finally {
    await pool.end().catch(() => undefined);
  }
}

if (await reachable()) {
  runStorageConformance({
    name: "postgres",
    // A pool hands each transaction its own connection, so calls issued together
    // really do run at the same time.
    concurrent: true,
    create: async () => {
      const storage = await PostgresStorage.open(connectionString);
      // Each test starts from an empty schema so the two backends see the same
      // initial conditions.
      await storage.resetForTesting();
      return {
        storage,
        dispose: async () => {
          await storage.close();
        },
      };
    },
  });
} else {
  describe("storage conformance — postgres", () => {
    it(
      "skipped: no PostgreSQL reachable at SENSUS_TEST_DATABASE_URL",
      { skip: true },
      () => undefined,
    );
  });
}
