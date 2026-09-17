import assert from "node:assert/strict";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { describe, it } from "node:test";
import { Pool } from "pg";
import { createConsumerContext } from "../src/access.js";
import { createSensusHttpServer } from "../src/http.js";
import { observationSchema } from "../src/protocol.js";
import { createSensusMcpHttpServer } from "../src/mcp-http.js";
import { openStorage, selectStorage } from "../src/storage-factory.js";
import { PostgresStorage } from "../src/storage-postgres.js";
import { SensusWorld } from "../src/world.js";
import { change, gitlabScenario, team } from "./fixtures.js";

/**
 * Proves the PostgreSQL backend is reachable from the running services, not just
 * from the storage conformance suite.
 *
 * It uses its own database so `node --test` can run it in parallel with the
 * conformance suite without the two resetting each other's schema.
 */
const connectionString =
  process.env.SENSUS_TEST_DATABASE_URL_E2E ??
  `postgres://${process.env.USER ?? "postgres"}@127.0.0.1:5432/sensus_e2e_test`;

async function ensureDatabase(url: string): Promise<boolean> {
  const target = new URL(url);
  const database = target.pathname.replace(/^\//, "");
  const maintenance = new URL(url);
  maintenance.pathname = "/postgres";
  const pool = new Pool({
    connectionString: maintenance.toString(),
    connectionTimeoutMillis: 2000,
  });
  try {
    const existing = await pool.query(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [database],
    );
    if (!existing.rowCount) {
      await pool.query(`CREATE DATABASE "${database.replaceAll('"', '""')}"`);
    }
    return true;
  } catch {
    return false;
  } finally {
    await pool.end().catch(() => undefined);
  }
}

if (await ensureDatabase(connectionString)) {
  describe("Sensus running on PostgreSQL", () => {
    it("selects the backend from SENSUS_DATABASE_URL", async () => {
      assert.equal(selectStorage({}).kind, "sqlite");
      assert.equal(
        selectStorage({ SENSUS_DB_PATH: "/tmp/x.db" }).target,
        "/tmp/x.db",
      );

      const selection = selectStorage({ SENSUS_DATABASE_URL: connectionString });
      assert.equal(selection.kind, "postgres");

      const storage = await openStorage(selection);
      assert.ok(
        storage instanceof PostgresStorage,
        "SENSUS_DATABASE_URL must select the PostgreSQL backend",
      );
      await storage.close();
    });

    it("serves the ingestion API and the read model from PostgreSQL", async () => {
      const storage = await PostgresStorage.open(connectionString);
      await storage.resetForTesting();
      const server = createSensusHttpServer({
        store: storage,
        apiKey: "test-secret",
        defaultTenantId: "acme",
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const { port } = server.address() as AddressInfo;
      const baseUrl = `http://127.0.0.1:${port}`;
      const headers = {
        authorization: "Bearer test-secret",
        "content-type": "application/json",
      };

      try {
        const batch = await fetch(`${baseUrl}/v1/observations/batch`, {
          method: "POST",
          headers,
          body: JSON.stringify({ observations: gitlabScenario() }),
        });
        assert.equal(batch.status, 200);
        const ingested = (await batch.json()) as {
          accepted: number;
          rejected: number;
        };
        assert.equal(ingested.accepted, gitlabScenario().length);
        assert.equal(ingested.rejected, 0);

        const read = await fetch(`${baseUrl}/v1/observations/obs_change`, {
          headers: {
            authorization: "Bearer test-secret",
            "x-sensus-tenant": "acme",
          },
        });
        assert.equal(read.status, 200);
        const readResult = (await read.json()) as {
          observation: { observation_id: string; kind: string };
        };
        assert.equal(readResult.observation.observation_id, "obs_change");
        assert.equal(readResult.observation.kind, "entity.observed");

        // The read model resolves the same rows through the contract.
        const world = new SensusWorld(storage, "acme");
        const view = await world.observe({
          scope: team,
          include: ["state", "changes", "signals"],
          limit: 50,
        });
        const signals = view.signals as Array<{ type: string }>;
        const changes = view.changes as Array<{ metric: string }>;
        assert.equal(signals[0]?.type, "metric.significant_increase");
        assert.equal(changes[0]?.metric, "software.review_wait_time");

        // A rule configured over HTTP takes effect against rows already stored.
        const rule = await fetch(`${baseUrl}/v1/signal-rules`, {
          method: "POST",
          headers,
          body: JSON.stringify({
            tenant_id: "acme",
            rule: {
              rule_id: "pg_rule",
              name: "Review wait over seven hours",
              enabled: true,
              applies_to: {
                metric: "software.review_wait_time",
                dimensions: {},
              },
              condition: {
                kind: "threshold",
                operator: "gt",
                value: 7,
                for_samples: 1,
              },
              signal_type: "software.review_wait_slo_breach",
              severity: "critical",
              confidence: 1,
            },
          }),
        });
        assert.equal(rule.status, 200);

        const afterRule = await world.observe({
          scope: team,
          include: ["signals"],
          limit: 50,
        });
        const raised = afterRule.signals as Array<{ type: string }>;
        assert.equal(raised[0]?.type, "software.review_wait_slo_breach");
      } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await storage.close();
      }
    });

    it("serves the MCP HTTP transport from PostgreSQL", async () => {
      const storage = await PostgresStorage.open(connectionString);
      await storage.resetForTesting();
      await storage.ingestMany(gitlabScenario());

      const { server, close } = createSensusMcpHttpServer({
        store: storage,
        tenantId: "acme",
        fallbackConsumer: createConsumerContext({
          principals: ["role:agent", "team:security", "team:payments"],
          clearance: "restricted",
        }),
      });
      server.listen(0, "127.0.0.1");
      await once(server, "listening");
      const { port } = server.address() as AddressInfo;

      try {
        const response = await fetch(`http://127.0.0.1:${port}/mcp`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            accept: "application/json, text/event-stream",
          },
          body: JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            method: "tools/call",
            params: {
              name: "observe",
              arguments: { scope: team, include: ["changes"], limit: 50 },
            },
          }),
        });
        assert.equal(response.status, 200);
        const body = await response.text();
        assert.match(
          body,
          /software\.review_wait_time/,
          "the MCP tool answer should carry data read from PostgreSQL",
        );
      } finally {
        await close();
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await storage.close();
      }
    });

    it("keeps tenant isolation when two tenants share one database", async () => {
      const storage = await PostgresStorage.open(connectionString);
      await storage.resetForTesting();
      try {
        for (const observation of gitlabScenario()) {
          await storage.ingest(observation);
        }
        await storage.ingest(
          observationSchema.parse({
            spec_version: "sensus/0.1",
            observation_id: "obs_globex",
            tenant_id: "globex",
            kind: "entity.observed",
            subject: change,
            occurred_at: "2026-09-15T09:00:00Z",
            observed_at: "2026-09-15T09:00:01Z",
            source: { system: "gitlab", instance: "globex-gitlab" },
            data: { name: "Globex change", lifecycle: "active" },
          }),
        );

        const acme = new SensusWorld(storage, "acme");
        const globex = new SensusWorld(storage, "globex");
        const acmeView = await acme.observe({
          scope: change,
          include: ["state"],
          limit: 50,
        });
        const globexView = await globex.observe({
          scope: change,
          include: ["state"],
          limit: 50,
        });

        assert.equal(
          (acmeView.entity as { name?: string }).name,
          "Add batch refund support",
        );
        assert.equal(
          (globexView.entity as { name?: string }).name,
          "Globex change",
        );
      } finally {
        await storage.close();
      }
    });
  });
} else {
  describe("Sensus running on PostgreSQL", () => {
    it(
      "skipped: no PostgreSQL reachable for the end-to-end suite",
      { skip: true },
      () => undefined,
    );
  });
}
