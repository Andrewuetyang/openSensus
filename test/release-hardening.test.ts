import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { after, describe, it } from "node:test";
import Database from "better-sqlite3";
import { createConsumerContext } from "../src/access.js";
import { createSensusHttpServer } from "../src/http.js";
import {
  AnonymousFallbackResolver,
  UnauthenticatedError,
} from "../src/identity.js";
import {
  identityConfigSchema,
  resolverFromConfig,
} from "../src/identity-config.js";
import { observationSchema, type Observation } from "../src/protocol.js";
import { SensusStore } from "../src/store.js";
import { SqliteStorage } from "../src/storage-sqlite.js";
import { change, gitlabScenario, repository } from "./fixtures.js";

/**
 * Regressions for the defects fixed before the 0.1.0 release. Each test here
 * failed against the pre-release code; they exist so the specific failure cannot
 * come back unnoticed.
 */
const scratchDirs: string[] = [];

function scratchDatabase(): string {
  const directory = mkdtempSync(join(tmpdir(), "sensus-test-"));
  scratchDirs.push(directory);
  return join(directory, "sensus.db");
}

after(() => {
  for (const directory of scratchDirs) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function stateObservation(input: {
  observationId: string;
  occurredAt: string;
  value: string;
}): Observation {
  return observationSchema.parse({
    spec_version: "sensus/0.1",
    observation_id: input.observationId,
    tenant_id: "acme",
    kind: "state.observed",
    subject: change,
    occurred_at: input.occurredAt,
    observed_at: input.occurredAt,
    source: { system: "gitlab", instance: "acme-gitlab" },
    data: {
      field: "software.review_status",
      operation: "set",
      value: input.value,
    },
  });
}

describe("timestamp normalization", () => {
  it("orders a non-UTC offset by instant, not by string", () => {
    const store = new SensusStore(":memory:");
    // 10:00+02:00 is 08:00Z, so the 09:00Z observation is the later one. Compared
    // as raw strings, "10:00:00+02:00" sorts after "09:00:00Z" and the later
    // value used to be discarded as a late arrival.
    store.ingest(
      stateObservation({
        observationId: "obs_offset_earlier",
        occurredAt: "2026-01-01T10:00:00+02:00",
        value: "waiting",
      }),
    );
    store.ingest(
      stateObservation({
        observationId: "obs_offset_later",
        occurredAt: "2026-01-01T09:00:00Z",
        value: "approved",
      }),
    );

    const states = store.getStates("acme", change);
    assert.equal(states.length, 1);
    assert.equal(states[0]!.value, "approved");
    assert.equal(states[0]!.occurred_at, "2026-01-01T09:00:00.000Z");
    store.close();
  });

  it("normalizes a stored instant to UTC", () => {
    const store = new SensusStore(":memory:");
    const observation = stateObservation({
      observationId: "obs_offset_only",
      occurredAt: "2026-03-01T12:30:00+05:30",
      value: "waiting",
    });
    assert.equal(observation.kind, "state.observed");
    assert.equal(observation.occurred_at, "2026-03-01T07:00:00.000Z");
    store.ingest(observation);
    assert.equal(
      store.getObservation("acme", "obs_offset_only")?.occurred_at,
      "2026-03-01T07:00:00.000Z",
    );
    store.close();
  });

  it("treats the same instant in two offsets as one payload", () => {
    const store = new SensusStore(":memory:");
    store.ingest(
      stateObservation({
        observationId: "obs_same_instant",
        occurredAt: "2026-01-01T10:00:00+02:00",
        value: "waiting",
      }),
    );
    const repeat = store.ingest(
      stateObservation({
        observationId: "obs_same_instant",
        occurredAt: "2026-01-01T08:00:00Z",
        value: "waiting",
      }),
    );
    assert.equal(repeat.status, "duplicate");
    store.close();
  });

  it("still rejects a genuine payload conflict", () => {
    const store = new SensusStore(":memory:");
    store.ingest(
      stateObservation({
        observationId: "obs_conflict",
        occurredAt: "2026-01-01T10:00:00Z",
        value: "waiting",
      }),
    );
    assert.throws(
      () =>
        store.ingest(
          stateObservation({
            observationId: "obs_conflict",
            occurredAt: "2026-01-01T10:00:00Z",
            value: "approved",
          }),
        ),
      /different payload/,
    );
    store.close();
  });
});

describe("field-level entity ACLs", () => {
  it("fails closed when a field has no recorded provenance", () => {
    const path = scratchDatabase();
    const subject = { type: "secret.thing", id: "s/1" };
    const store = new SensusStore(path);
    store.ingest(
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: "obs_restricted_attr",
        tenant_id: "acme",
        kind: "entity.observed",
        subject,
        occurred_at: "2026-01-01T01:00:00Z",
        observed_at: "2026-01-01T01:00:00Z",
        source: { system: "hris", instance: "acme-hris" },
        data: { attributes: { secret_salary: "TOP-SECRET" } },
        access: { classification: "restricted", allow: ["team:security"] },
      }),
    );
    store.ingest(
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: "obs_public_attr",
        tenant_id: "acme",
        kind: "entity.observed",
        subject,
        occurred_at: "2026-01-01T02:00:00Z",
        observed_at: "2026-01-01T02:00:00Z",
        source: { system: "hris", instance: "acme-hris" },
        data: { attributes: { public_note: "P" } },
        access: { classification: "public" },
      }),
    );
    store.close();

    // A database upgraded from before field_sources_json existed starts at `{}`
    // for every row, and only regains provenance for fields something writes
    // again. So the realistic post-upgrade shape is a field with no provenance
    // next to fields that have it — which is exactly the state that used to leak:
    // the unknown field was authorized by the entity's latest observation, an
    // Observation that never carried its value.
    const raw = new Database(path);
    const stored = raw
      .prepare("SELECT field_sources_json FROM entities LIMIT 1")
      .get() as { field_sources_json: string };
    const provenance = JSON.parse(stored.field_sources_json) as Record<string, string>;
    assert.ok(
      provenance["attribute:secret_salary"],
      "the restricted field did have provenance before the simulation",
    );
    delete provenance["attribute:secret_salary"];
    raw
      .prepare("UPDATE entities SET field_sources_json = ?")
      .run(JSON.stringify(provenance));
    raw.close();

    const reopened = new SensusStore(path);
    const low = createConsumerContext({
      principals: ["team:payments"],
      clearance: "internal",
    });
    const view = reopened.getEntity("acme", subject, low);
    assert.ok(view, "a field with provenance keeps the entity visible");
    assert.equal(
      (view!.attributes as Record<string, unknown>).secret_salary,
      undefined,
      "an unprovenanced field is not authorized by the entity's latest observation",
    );
    assert.equal(
      (view!.attributes as Record<string, unknown>).public_note,
      "P",
      "fields that do have provenance are unaffected",
    );

    // Fail-closed means fail-closed: with no provenance there is nothing to
    // authorize the field against, so even a cleared consumer does not see it
    // until a source re-asserts the value and provenance is recorded again.
    const security = createConsumerContext({
      principals: ["team:security"],
      clearance: "restricted",
    });
    assert.equal(
      (reopened.getEntity("acme", subject, security)!.attributes as Record<
        string,
        unknown
      >).secret_salary,
      undefined,
    );

    // Re-asserting the value records provenance again, and access returns.
    reopened.ingest(
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: "obs_restricted_attr_reasserted",
        tenant_id: "acme",
        kind: "entity.observed",
        subject,
        occurred_at: "2026-01-01T03:00:00Z",
        observed_at: "2026-01-01T03:00:00Z",
        source: { system: "hris", instance: "acme-hris" },
        data: { attributes: { secret_salary: "TOP-SECRET" } },
        access: { classification: "restricted", allow: ["team:security"] },
      }),
    );
    assert.equal(
      (reopened.getEntity("acme", subject, security)!.attributes as Record<
        string,
        unknown
      >).secret_salary,
      "TOP-SECRET",
    );
    reopened.close();
  });
});

describe("Signal evidence", () => {
  it("hides a Signal whose evidence reference cannot be interpreted", () => {
    const path = scratchDatabase();
    const store = new SensusStore(path);
    for (const observation of gitlabScenario()) store.ingest(observation);
    store.close();

    // Rewrite the generated Signal's evidence to a ref this runtime cannot
    // parse. It used to be treated as readable, so the Signal became visible to
    // anyone who could read its own policy.
    const raw = new Database(path);
    const row = raw
      .prepare("SELECT signal_id, payload_json FROM signals LIMIT 1")
      .get() as { signal_id: string; payload_json: string } | undefined;
    assert.ok(row, "the scenario produces a Signal");
    const payload = JSON.parse(row.payload_json) as {
      evidence: Array<{ type: string; ref: string }>;
    };
    payload.evidence = [{ type: "source_record", ref: "not-a-sensus-uri" }];
    raw
      .prepare("UPDATE signals SET payload_json = ? WHERE signal_id = ?")
      .run(JSON.stringify(payload), row.signal_id);
    raw.close();

    const reopened = new SensusStore(path);
    const low = createConsumerContext({
      principals: ["team:payments"],
      clearance: "internal",
    });
    assert.equal(
      reopened.getSignals("acme", repository, "open", low).length,
      0,
      "unparseable evidence is fail-closed",
    );
    assert.equal(
      reopened.getSignal("acme", row.signal_id, low),
      undefined,
    );
    reopened.close();
  });
});

describe("relation graph bounds", () => {
  it("never returns an edge whose endpoint is missing from nodes", () => {
    const store = new SensusStore(":memory:");
    for (const observation of gitlabScenario()) store.ingest(observation);

    const graph = store.traverseGraph(
      "acme",
      change,
      { direction: "both", maxDepth: 3, maxNodes: 1 },
      createConsumerContext({
        principals: ["role:agent"],
        clearance: "restricted",
      }),
    );
    assert.equal(graph.truncated, true);
    assert.equal(graph.nodes.length, 1);

    const present = new Set(graph.nodes.map((node) => `${node.type}\u001f${node.id}`));
    for (const edge of graph.edges) {
      const subject = edge.subject as { type: string; id: string };
      const target = edge.target as { type: string; id: string };
      assert.ok(
        present.has(`${subject.type}\u001f${subject.id}`),
        `edge subject ${subject.id} is missing from nodes`,
      );
      assert.ok(
        present.has(`${target.type}\u001f${target.id}`),
        `edge target ${target.id} is missing from nodes`,
      );
    }
    store.close();
  });
});

describe("SQLite duplicate handling across processes", () => {
  it("answers duplicate and conflict without surfacing a constraint error", () => {
    const path = scratchDatabase();
    const first = new SensusStore(path);
    const second = new SensusStore(path);

    const observation = stateObservation({
      observationId: "obs_shared",
      occurredAt: "2026-01-01T10:00:00Z",
      value: "waiting",
    });

    assert.equal(first.ingest(observation).status, "accepted");
    // A second connection sees the committed row, so this is the ordinary
    // duplicate path. The interleaving that used to escape it needs two OS
    // processes; what is asserted here is the contract either way: no raw
    // SQLITE_CONSTRAINT escapes as a 500.
    assert.equal(second.ingest(observation).status, "duplicate");

    assert.throws(
      () =>
        second.ingest(
          stateObservation({
            observationId: "obs_shared",
            occurredAt: "2026-01-01T10:00:00Z",
            value: "approved",
          }),
        ),
      (error: unknown) =>
        error instanceof Error && error.name === "ObservationConflictError",
    );
    first.close();
    second.close();
  });
});

describe("tenant resolution", () => {
  async function withServer(
    options: Parameters<typeof createSensusHttpServer>[0],
    body: (baseUrl: string) => Promise<void>,
  ): Promise<void> {
    const server = createSensusHttpServer(options);
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const { port } = server.address() as AddressInfo;
    try {
      await body(`http://127.0.0.1:${port}`);
    } finally {
      server.close();
      await once(server, "close");
    }
  }

  const observation = () =>
    observationSchema.parse({
      spec_version: "sensus/0.1",
      observation_id: "obs_tenant",
      tenant_id: "tenantB",
      kind: "entity.observed",
      subject: { type: "project", id: "tenantB/p1" },
      occurred_at: "2026-01-01T00:00:00Z",
      observed_at: "2026-01-01T00:00:00Z",
      source: { system: "gitlab", instance: "i" },
      data: { name: "secret project" },
    });

  const headers = {
    authorization: "Bearer test-secret",
    "content-type": "application/json",
  };

  it("refuses a request-named tenant when nothing is configured", async () => {
    const store = SqliteStorage.open(":memory:");
    await withServer({ store, apiKey: "test-secret" }, async (baseUrl) => {
      const write = await fetch(`${baseUrl}/v1/observations`, {
        method: "POST",
        headers,
        body: JSON.stringify(observation()),
      });
      assert.equal(write.status, 403);
      const error = (await write.json()) as { error: { code: string } };
      assert.equal(error.error.code, "PERMISSION_DENIED");

      const read = await fetch(`${baseUrl}/v1/observations/obs_tenant`, {
        headers: { ...headers, "x-sensus-tenant": "tenantB" },
      });
      assert.equal(read.status, 403);
    });
    await store.close();
  });

  it("honours a request-named tenant only when opted in", async () => {
    const store = SqliteStorage.open(":memory:");
    await withServer(
      { store, apiKey: "test-secret", allowRequestTenant: true },
      async (baseUrl) => {
        const write = await fetch(`${baseUrl}/v1/observations`, {
          method: "POST",
          headers,
          body: JSON.stringify(observation()),
        });
        assert.equal(write.status, 200);

        const read = await fetch(`${baseUrl}/v1/observations/obs_tenant`, {
          headers: { ...headers, "x-sensus-tenant": "tenantB" },
        });
        assert.equal(read.status, 200);
      },
    );
    await store.close();
  });

  it("keeps a configured tenant authoritative", async () => {
    const store = SqliteStorage.open(":memory:");
    await withServer(
      {
        store,
        apiKey: "test-secret",
        defaultTenantId: "tenantA",
        allowRequestTenant: true,
      },
      async (baseUrl) => {
        const mismatch = await fetch(`${baseUrl}/v1/observations/obs_tenant`, {
          headers: { ...headers, "x-sensus-tenant": "tenantB" },
        });
        assert.equal(mismatch.status, 403);
      },
    );
    await store.close();
  });
});

describe("anonymous identity configuration", () => {
  it("serves a credential-less request as the configured anonymous identity", async () => {
    const config = identityConfigSchema.parse({
      api_key: "shared-secret",
      api_key_identity: { principals: ["role:producer"], clearance: "confidential" },
      anonymous: { principals: ["role:reader"], clearance: "public" },
    });
    const resolver = resolverFromConfig(config);
    assert.ok(resolver instanceof AnonymousFallbackResolver);
    assert.equal(resolver.acceptsAnonymous, true);

    const anonymous = await resolver.resolve({ authorization: undefined });
    assert.deepEqual([...anonymous.consumer.principals], ["role:reader"]);
    assert.equal(anonymous.consumer.clearance, "public");

    const authenticated = await resolver.resolve({
      authorization: "Bearer shared-secret",
    });
    assert.deepEqual(
      [...authenticated.consumer.principals],
      ["role:producer"],
      "the api key's identity is api_key_identity, not the anonymous block",
    );
    assert.equal(authenticated.consumer.clearance, "confidential");
  });

  it("rejects an unverifiable credential rather than downgrading it", async () => {
    const resolver = resolverFromConfig(
      identityConfigSchema.parse({
        api_key: "shared-secret",
        anonymous: { principals: ["role:reader"] },
      }),
    );
    await assert.rejects(
      () => resolver.resolve({ authorization: "Bearer wrong" }),
      UnauthenticatedError,
    );
  });

  it("rejects a credential when only anonymous is configured", async () => {
    const resolver = resolverFromConfig(
      identityConfigSchema.parse({ anonymous: { principals: ["role:reader"] } }),
    );
    await assert.rejects(
      () => resolver.resolve({ authorization: "Bearer anything" }),
      UnauthenticatedError,
    );
  });

  it("requires a credential when anonymous is omitted", async () => {
    const resolver = resolverFromConfig(
      identityConfigSchema.parse({ api_key: "shared-secret" }),
    );
    assert.notEqual(resolver.acceptsAnonymous, true);
    await assert.rejects(
      () => resolver.resolve({ authorization: undefined }),
      UnauthenticatedError,
    );
  });

  it("rejects a configuration that could never serve a request", () => {
    const result = identityConfigSchema.safeParse({ mapping: {} });
    assert.equal(result.success, false);
  });
});
