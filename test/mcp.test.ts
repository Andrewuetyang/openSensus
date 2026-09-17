import assert from "node:assert/strict";
import { Client, InMemoryTransport } from "@modelcontextprotocol/client";
import { describe, it } from "node:test";
import { buildMcpServer } from "../src/mcp.js";
import { SqliteStorage } from "../src/storage-sqlite.js";
import { SensusWorld } from "../src/world.js";
import { gitlabScenario, team } from "./fixtures.js";

describe("Sensus MCP", () => {
  it("advertises six tools and lets an Agent observe the world", async () => {
    const store = SqliteStorage.open(":memory:");
    for (const observation of gitlabScenario()) await store.ingest(observation);

    const server = buildMcpServer(new SensusWorld(store, "acme"));
    const client = new Client({ name: "sensus-test", version: "0.1.0" });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([
      server.connect(serverTransport),
      client.connect(clientTransport),
    ]);

    const tools = await client.listTools();
    assert.deepEqual(
      tools.tools.map((tool) => tool.name).sort(),
      ["compare", "get_evidence", "inspect", "observe", "query", "timeline"],
    );

    const result = await client.callTool({
      name: "observe",
      arguments: {
        scope: team,
        include: ["state", "changes", "signals"],
        limit: 50,
      },
    });
    assert.equal(result.isError, undefined);
    const text = result.content.find((item) => item.type === "text");
    assert.ok(text && "text" in text);
    const payload = JSON.parse(text.text) as {
      signals: Array<{ type: string }>;
    };
    assert.equal(payload.signals[0]?.type, "metric.significant_increase");

    await client.close();
    await server.close();
    await store.close();
  });
});

