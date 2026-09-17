import assert from "node:assert/strict";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { describe, it } from "node:test";
import { createSensusHttpServer } from "../src/http.js";
import { SqliteStorage } from "../src/storage-sqlite.js";
import { gitlabScenario } from "./fixtures.js";

describe("Sensus HTTP API", () => {
  it("accepts a batch and reads an observation back", async () => {
    const store = SqliteStorage.open(":memory:");
    const server = createSensusHttpServer({
      store,
      apiKey: "test-secret",
      defaultTenantId: "acme",
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address() as AddressInfo;
    const baseUrl = `http://127.0.0.1:${address.port}`;

    const unauthorized = await fetch(`${baseUrl}/health`);
    assert.equal(unauthorized.status, 401);

    const response = await fetch(`${baseUrl}/v1/observations/batch`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-secret",
        "content-type": "application/json",
      },
      body: JSON.stringify({ observations: gitlabScenario() }),
    });
    assert.equal(response.status, 200);
    const result = (await response.json()) as {
      accepted: number;
      rejected: number;
    };
    assert.equal(result.accepted, gitlabScenario().length);
    assert.equal(result.rejected, 0);

    const readResponse = await fetch(`${baseUrl}/v1/observations/obs_change`, {
      headers: {
        authorization: "Bearer test-secret",
        "x-sensus-tenant": "acme",
      },
    });
    assert.equal(readResponse.status, 200);
    const readResult = (await readResponse.json()) as {
      observation: { observation_id: string };
    };
    assert.equal(readResult.observation.observation_id, "obs_change");

    await new Promise<void>((resolve) => server.close(() => resolve()));
    await store.close();
  });

  it("manages reconciliation sessions and configurable Signal rules", async () => {
    const store = SqliteStorage.open(":memory:");
    const server = createSensusHttpServer({
      store,
      apiKey: "test-secret",
      defaultTenantId: "acme",
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address() as AddressInfo;
    const baseUrl = `http://127.0.0.1:${address.port}`;
    const headers = {
      authorization: "Bearer test-secret",
      "content-type": "application/json",
    };

    const start = await fetch(`${baseUrl}/v1/syncs`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        tenant_id: "acme",
        sync_id: "sync_http",
        mode: "snapshot",
        source: { system: "hris", instance: "acme-hris" },
        authoritative_deletion: true,
      }),
    });
    assert.equal(start.status, 201);

    const teamObservation = gitlabScenario()[0]!;
    const ingest = await fetch(`${baseUrl}/v1/observations`, {
      method: "POST",
      headers: { ...headers, "sensus-sync-id": "sync_http" },
      body: JSON.stringify(teamObservation),
    });
    assert.equal(ingest.status, 200);

    const complete = await fetch(`${baseUrl}/v1/syncs/sync_http/complete`, {
      method: "POST",
      headers,
      body: JSON.stringify({ record_count: 1 }),
    });
    assert.equal(complete.status, 200);
    const completed = (await complete.json()) as {
      sync: { status: string; actual_record_count: number };
    };
    assert.equal(completed.sync.status, "completed");
    assert.equal(completed.sync.actual_record_count, 1);

    const ruleResponse = await fetch(`${baseUrl}/v1/signal-rules`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        tenant_id: "acme",
        rule: {
          rule_id: "http_rule",
          name: "HTTP configured rule",
          enabled: true,
          applies_to: { metric: "software.review_wait_time", dimensions: {} },
          condition: {
            kind: "threshold",
            operator: "gt",
            value: 10,
            for_samples: 1,
          },
          signal_type: "software.review_wait_high",
          severity: "warning",
          confidence: 0.9,
        },
      }),
    });
    assert.equal(ruleResponse.status, 200);
    const rulesResponse = await fetch(`${baseUrl}/v1/signal-rules`, {
      headers: { authorization: "Bearer test-secret" },
    });
    const rules = (await rulesResponse.json()) as { rules: Array<{ rule_id: string }> };
    assert.equal(rules.rules[0]?.rule_id, "http_rule");

    await new Promise<void>((resolve) => server.close(() => resolve()));
    await store.close();
  });
});
