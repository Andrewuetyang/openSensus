import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { after, describe, it } from "node:test";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { createConsumerContext } from "../src/access.js";
import {
  defaultIdentityMapping,
  identityMappingSchema,
  mapIdentity,
  UnauthenticatedError,
  type IdentityMapping,
  type VerifiedIdentity,
} from "../src/identity.js";
import { OidcIdentityResolver } from "../src/identity-oidc.js";
import { createSensusMcpHttpServer } from "../src/mcp-http.js";
import { observationSchema } from "../src/protocol.js";
import { SqliteStorage } from "../src/storage-sqlite.js";
import { change, gitlabScenario, team } from "./fixtures.js";

const issuer = "https://idp.test/";
const audience = "sensus";

function verified(overrides: Partial<VerifiedIdentity> = {}): VerifiedIdentity {
  return {
    subject: "alice",
    groups: [],
    claims: {},
    ...overrides,
  };
}

function mapping(input: unknown): IdentityMapping {
  return identityMappingSchema.parse(input);
}

describe("identity mapping", () => {
  it("derives principals from the subject and configured claims", () => {
    const resolved = mapIdentity(
      verified({
        subject: "alice",
        groups: ["payments-team", "eng-leads"],
        claims: { groups: ["payments-team", "eng-leads"], email: "alice@acme.test" },
      }),
      mapping({
        principals: [
          {
            claim: "groups",
            prefix: "group:",
            map: { "payments-team": "team:payments" },
          },
          { claim: "email" },
        ],
      }),
    );

    assert.deepEqual(
      [...resolved.consumer.principals].sort(),
      ["group:eng-leads", "team:payments", "user:alice", "alice@acme.test"].sort(),
    );
    assert.equal(resolved.subject, "alice");
  });

  it("caps clearance at the configured ceiling", () => {
    const identity = verified({
      groups: ["sec-cleared-3"],
      claims: { groups: ["sec-cleared-3"] },
    });

    const capped = mapIdentity(
      identity,
      mapping({
        clearance: {
          from_groups: { "sec-cleared-3": "restricted" },
          ceiling: "internal",
        },
      }),
    );
    assert.equal(capped.consumer.clearance, "internal");

    const allowed = mapIdentity(
      identity,
      mapping({
        clearance: {
          from_groups: { "sec-cleared-3": "restricted" },
          ceiling: "restricted",
        },
      }),
    );
    assert.equal(allowed.consumer.clearance, "restricted");
  });

  it("takes the most privileged matching group, then applies the default", () => {
    const mapped = mapIdentity(
      verified({
        groups: ["viewers", "analysts"],
        claims: { groups: ["viewers", "analysts"] },
      }),
      mapping({
        clearance: {
          from_groups: { viewers: "internal", analysts: "confidential" },
          default: "public",
        },
      }),
    );
    assert.equal(mapped.consumer.clearance, "confidential");

    const fallback = mapIdentity(
      verified({ claims: { groups: ["unlisted"] } }),
      mapping({ clearance: { default: "public" } }),
    );
    assert.equal(fallback.consumer.clearance, "public");
  });

  it("ignores an unrecognized clearance claim rather than granting it", () => {
    const resolved = mapIdentity(
      verified({ claims: { clearance: "super-secret" } }),
      mapping({ clearance: { claim: "clearance", default: "internal" } }),
    );
    assert.equal(resolved.consumer.clearance, "internal");
  });

  it("accepts a tenant only when the token names an allowed one", () => {
    const config = mapping({
      tenant: { claim: "tenant_id", allowed: ["acme"] },
    });

    const ok = mapIdentity(
      verified({ claims: { tenant_id: "acme" } }),
      config,
    );
    assert.equal(ok.tenantId, "acme");

    assert.throws(
      () => mapIdentity(verified({ claims: { tenant_id: "globex" } }), config),
      /not in the allowed list/,
    );
  });

  it("leaves the tenant unset when the token does not carry one", () => {
    const resolved = mapIdentity(
      verified({ claims: {} }),
      mapping({ tenant: { claim: "tenant_id", allowed: ["acme"] } }),
    );
    assert.equal(resolved.tenantId, undefined);
  });

  it("refuses to configure a tenant claim without an allowlist", () => {
    assert.throws(
      () => identityMappingSchema.parse({ tenant: { claim: "tenant_id" } }),
      /allowed is required/,
    );
  });

  it("never produces a system consumer, whatever the claims say", () => {
    const resolved = mapIdentity(
      verified({
        claims: { system: true, clearance: "restricted", groups: ["admin"] },
      }),
      mapping({
        clearance: { claim: "clearance", ceiling: "restricted" },
        principals: [{ claim: "groups" }],
      }),
    );
    assert.equal(resolved.consumer.system, false);
  });

  it("applies defaults when no mapping is configured", () => {
    const resolved = mapIdentity(verified({ subject: "bob" }));
    assert.deepEqual([...resolved.consumer.principals], ["user:bob"]);
    assert.equal(resolved.consumer.clearance, "internal");
    assert.equal(identityMappingSchema.parse({}).subject_prefix, "user:");
    assert.equal(defaultIdentityMapping.clearance.ceiling, "restricted");
  });
});

type Provider = {
  issuer: string;
  sign: (claims: Record<string, unknown>, options?: {
    expiresIn?: string;
    subject?: string;
    audience?: string;
    issuer?: string;
    kid?: string;
  }) => Promise<string>;
  server: Server;
};

/**
 * Every provider started during the run. Each `it` starts its own, so they all
 * have to be closed or the listener keeps the test process alive after the last
 * assertion.
 */
const startedProviders: Provider[] = [];

async function closeStartedProviders(): Promise<void> {
  const open = startedProviders.splice(0);
  await Promise.all(
    open.map(async (instance) => {
      instance.server.close();
      await once(instance.server, "close").catch(() => undefined);
    }),
  );
}

async function startProvider(): Promise<Provider> {
  const { publicKey, privateKey } = await generateKeyPair("RS256");
  const jwk = { ...(await exportJWK(publicKey)), kid: "test-key", alg: "RS256", use: "sig" };

  // Captured per provider rather than read from shared state: with more than one
  // provider alive, a module-level reference would hand an earlier provider's
  // discovery document a later provider's issuer.
  let issuer = "";

  const server = createServer((request, response) => {
    if (request.url?.endsWith("/.well-known/openid-configuration")) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(
        JSON.stringify({
          issuer,
          // jwks_uri is an absolute URL. `issuer` carries no trailing slash, so
          // the separator has to be explicit.
          jwks_uri: `${issuer}/jwks`,
        }),
      );
      return;
    }
    if (request.url === "/jwks") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ keys: [jwk] }));
      return;
    }
    response.writeHead(404);
    response.end();
  });

  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address() as AddressInfo;
  const base = `http://127.0.0.1:${port}`;
  issuer = base;

  const instance: Provider = {
    issuer: base,
    server,
    sign: async (claims, options = {}) =>
      await new SignJWT(claims)
        .setProtectedHeader({ alg: "RS256", kid: options.kid ?? "test-key" })
        .setIssuer(options.issuer ?? base)
        .setAudience(options.audience ?? audience)
        .setSubject(options.subject ?? "alice")
        .setIssuedAt()
        .setExpirationTime(options.expiresIn ?? "5m")
        .sign(privateKey),
  };
  startedProviders.push(instance);
  return instance;
}

async function resolverFor(
  providerInstance: Provider,
  config: unknown = {},
): Promise<OidcIdentityResolver> {
  const parsed = mapping(config);
  return new OidcIdentityResolver({
    issuer: providerInstance.issuer,
    audience,
    mapping: parsed,
  });
}

describe("OIDC identity resolver", () => {
  it("verifies a token through discovery and maps its claims", async () => {
    const idp = await startProvider();
    const resolver = await resolverFor(idp, {
      principals: [{ claim: "groups", prefix: "team:", map: { "payments-team": "team:payments" } }],
      clearance: { from_groups: { "payments-team": "confidential" }, ceiling: "confidential" },
    });

    const token = await idp.sign({ groups: ["payments-team"] }, { subject: "alice" });
    const resolved = await resolver.resolve({ authorization: `Bearer ${token}` });

    assert.deepEqual(
      [...resolved.consumer.principals].sort(),
      ["team:payments", "user:alice"].sort(),
    );
    assert.equal(resolved.consumer.clearance, "confidential");
    assert.equal(resolved.subject, "alice");
    assert.ok(resolved.expiresAt && resolved.expiresAt > Date.now() / 1000);
  });

  it("rejects missing, malformed and non-bearer credentials", async () => {
    const idp = await startProvider();
    const resolver = await resolverFor(idp);

    for (const authorization of [undefined, "", "Basic abc", "Bearer"]) {
      await assert.rejects(
        () => resolver.resolve({ authorization }),
        UnauthenticatedError,
        `expected ${JSON.stringify(authorization)} to be rejected`,
      );
    }
  });

  it("rejects an expired token", async () => {
    const idp = await startProvider();
    const resolver = await resolverFor(idp);
    const token = await idp.sign({}, { expiresIn: "-1m" });

    await assert.rejects(
      () => resolver.resolve({ authorization: `Bearer ${token}` }),
      UnauthenticatedError,
    );
  });

  it("rejects a token for another audience or issuer", async () => {
    const idp = await startProvider();
    const resolver = await resolverFor(idp);

    const wrongAudience = await idp.sign({}, { audience: "someone-else" });
    await assert.rejects(
      () => resolver.resolve({ authorization: `Bearer ${wrongAudience}` }),
      UnauthenticatedError,
    );

    const wrongIssuer = await idp.sign({}, { issuer: "https://evil.test/" });
    await assert.rejects(
      () => resolver.resolve({ authorization: `Bearer ${wrongIssuer}` }),
      UnauthenticatedError,
    );
  });

  it("rejects a token signed by an unknown key", async () => {
    const idp = await startProvider();
    const resolver = await resolverFor(idp);
    const token = await idp.sign({}, { kid: "not-in-the-jwks" });

    await assert.rejects(
      () => resolver.resolve({ authorization: `Bearer ${token}` }),
      UnauthenticatedError,
    );
  });

  it("rejects a token with a tampered signature", async () => {
    const idp = await startProvider();
    const resolver = await resolverFor(idp);
    const token = await idp.sign({ groups: ["payments-team"] });
    const [header, payload, signature] = token.split(".");
    const tampered = `${header}.${payload}.${signature!.slice(0, -4)}AAAA`;

    await assert.rejects(
      () => resolver.resolve({ authorization: `Bearer ${tampered}` }),
      UnauthenticatedError,
    );
  });

  after(async () => {
    await closeStartedProviders();
  });
});

describe("MCP over Streamable HTTP with per-request identity", () => {
  it("serves a different world view to each authenticated caller", async () => {
    const idp = await startProvider();
    const store = SqliteStorage.open(":memory:");
    for (const observation of gitlabScenario()) await store.ingest(observation);
    await store.ingest(
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: "obs_change_secret",
        tenant_id: "acme",
        kind: "entity.observed",
        subject: change,
        occurred_at: "2026-09-16T11:00:00Z",
        observed_at: "2026-09-16T11:00:01Z",
        source: { system: "gitlab", instance: "acme-gitlab" },
        data: { attributes: { security_risk: "critical" } },
        access: { classification: "confidential", allow: ["team:security"] },
      }),
    );

    const resolver = new OidcIdentityResolver({
      issuer: idp.issuer,
      audience,
      jwksUri: `${idp.issuer}/jwks`,
      mapping: mapping({
        principals: [
          { claim: "groups", prefix: "team:", map: { security: "team:security" } },
        ],
        clearance: { default: "confidential" },
      }),
    });

    const { server, close } = createSensusMcpHttpServer({
      store,
      tenantId: "acme",
      fallbackConsumer: createConsumerContext({ principals: ["role:agent"] }),
      resolver,
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const { port } = server.address() as AddressInfo;
    const url = new URL(`http://127.0.0.1:${port}/mcp`);

    const inspectAs = async (token: string) => {
      const client = new Client({ name: "identity-test", version: "0.1.0" });
      const transport = new StreamableHTTPClientTransport(url, {
        authProvider: { token: async () => token },
      });
      await client.connect(transport);
      const result = await client.callTool({
        name: "inspect",
        arguments: { kind: "entity", entity: change, include: ["state"] },
      });
      await client.close();
      const text = result.content.find((item) => item.type === "text");
      assert.ok(text && "text" in text);
      return JSON.parse(text.text) as {
        entity: { attributes: Record<string, unknown> };
      };
    };

    const ordinaryToken = await idp.sign({ groups: ["payments"] });
    const securityToken = await idp.sign({ groups: ["security"] });

    const ordinary = await inspectAs(ordinaryToken);
    assert.equal(ordinary.entity.attributes.security_risk, undefined);

    const security = await inspectAs(securityToken);
    assert.equal(security.entity.attributes.security_risk, "critical");

    await close();
    server.close();
    await once(server, "close");
    await store.close();
  });

  it("refuses an unauthenticated request when a resolver is configured", async () => {
    const idp = await startProvider();
    const store = SqliteStorage.open(":memory:");
    for (const observation of gitlabScenario()) await store.ingest(observation);

    const resolver = new OidcIdentityResolver({
      issuer: idp.issuer,
      audience,
      jwksUri: `${idp.issuer}/jwks`,
    });

    const { server, close } = createSensusMcpHttpServer({
      store,
      tenantId: "acme",
      fallbackConsumer: createConsumerContext({ principals: ["role:agent"] }),
      resolver,
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const { port } = server.address() as AddressInfo;

    const response = await fetch(`http://127.0.0.1:${port}/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
    });
    assert.equal(response.status, 401);

    await close();
    server.close();
    await once(server, "close");
    await store.close();
  });

  it("rejects a token whose tenant differs from the pinned one", async () => {
    const idp = await startProvider();
    const store = SqliteStorage.open(":memory:");

    const resolver = new OidcIdentityResolver({
      issuer: idp.issuer,
      audience,
      jwksUri: `${idp.issuer}/jwks`,
      mapping: mapping({ tenant: { claim: "tenant_id", allowed: ["globex"] } }),
    });

    const { server, close } = createSensusMcpHttpServer({
      store,
      tenantId: "acme",
      fallbackConsumer: createConsumerContext({ principals: ["role:agent"] }),
      resolver,
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const { port } = server.address() as AddressInfo;

    const token = await idp.sign({ tenant_id: "globex" });
    const response = await fetch(`http://127.0.0.1:${port}/mcp`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
        accept: "application/json, text/event-stream",
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list" }),
    });
    assert.equal(response.status, 403);

    await close();
    server.close();
    await once(server, "close");
    await store.close();
  });

  after(async () => {
    await closeStartedProviders();
  });
});

describe("identity does not disturb single-tenant behaviour", () => {
  it("keeps the standard agent principal readable for unscoped data", () => {
    const resolved = mapIdentity(verified({ subject: "agent-1" }));
    assert.deepEqual([...resolved.consumer.principals], ["user:agent-1"]);
    assert.equal(
      team.type,
      "organization.team",
      "fixture import keeps the tree honest",
    );
  });
});
